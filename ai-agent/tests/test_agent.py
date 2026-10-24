"""Unit tests for the AI triage agent. Kafka and CockroachDB are fully mocked."""
import json
import os
from decimal import Decimal
from unittest import mock

import pytest

import agent
from agent import (
    AIIncidentTriager,
    CockroachDBContextRetriever,
    java_string_literal,
    safe_class_suffix,
    save_generated_test,
)

BUDGET_EVENT = {
    "eventType": "AD_BUDGET_EXHAUSTED_ANOMALY",
    "campaignId": "CMP-2026-001",
    "advertiser": "Acme Streaming",
    "remainingBudget": 0.0,
    "active": True,
    "timestamp": 1761177600000,
}


# ---------- CockroachDB context retrieval ----------

def test_retriever_builds_connection_string_from_config():
    assert CockroachDBContextRetriever().conn_str == (
        f"postgresql://root@{agent.COCKROACH_HOST}:{agent.COCKROACH_PORT}/{agent.COCKROACH_DB}?sslmode=disable"
    )


def test_retriever_maps_row_to_dict_and_closes_connection():
    row = ("CMP-1", "Acme", "US_ALL", Decimal("12.50"), Decimal("15.00"), True, 42)
    conn = mock.MagicMock()
    conn.cursor.return_value.fetchone.return_value = row
    with mock.patch.object(agent.psycopg2, "connect", return_value=conn) as connect:
        ctx = CockroachDBContextRetriever().get_campaign_context("CMP-1")

    connect.assert_called_once()
    sql, params = conn.cursor.return_value.execute.call_args.args
    assert "FROM ad_campaigns WHERE campaign_id = %s" in sql
    assert params == ("CMP-1",)  # parameterized, not string-formatted
    assert ctx == {
        "campaign_id": "CMP-1",
        "advertiser_name": "Acme",
        "target_demographic": "US_ALL",
        "remaining_budget": 12.5,
        "target_cpm": 15.0,
        "active": True,
        "current_impression_count": 42,
    }
    assert isinstance(ctx["remaining_budget"], float)
    conn.cursor.return_value.close.assert_called_once()
    conn.close.assert_called_once()


def test_retriever_returns_empty_dict_when_campaign_missing():
    conn = mock.MagicMock()
    conn.cursor.return_value.fetchone.return_value = None
    with mock.patch.object(agent.psycopg2, "connect", return_value=conn):
        assert CockroachDBContextRetriever().get_campaign_context("NOPE") == {}


def test_retriever_swallows_db_errors(capsys):
    with mock.patch.object(agent.psycopg2, "connect", side_effect=Exception("connection refused")):
        assert CockroachDBContextRetriever().get_campaign_context("CMP-1") == {}
    assert "connection refused" in capsys.readouterr().out


# ---------- RCA generation ----------

def test_rca_budget_exhausted_prefers_db_ground_truth():
    rca = AIIncidentTriager()._generate_rca(
        "AD_BUDGET_EXHAUSTED_ANOMALY", BUDGET_EVENT, {"remaining_budget": 3.25, "active": False}
    )
    assert "CMP-2026-001" in rca
    assert "Acme Streaming" in rca
    assert "$3.25" in rca
    assert "'False'" in rca


def test_rca_budget_exhausted_falls_back_to_event_when_no_db_context():
    rca = AIIncidentTriager()._generate_rca("AD_BUDGET_EXHAUSTED_ANOMALY", BUDGET_EVENT, {})
    assert "$0.0" in rca
    assert "'True'" in rca


def test_rca_unknown_event_type_is_generic():
    rca = AIIncidentTriager()._generate_rca("CAMPAIGN_NOT_FOUND", {"campaignId": "X"}, {})
    assert rca.startswith("ROOT CAUSE ANALYSIS (RCA):")
    assert "CAMPAIGN_NOT_FOUND for Campaign 'X'" in rca


# ---------- JUnit generation ----------

def test_junit_test_has_valid_class_name_and_targets_campaign():
    code = AIIncidentTriager()._generate_junit_test("AD_BUDGET_EXHAUSTED_ANOMALY", "CMP-2026-001", {})
    assert code.startswith("package com.netflix.ads.generated;")
    assert "public class AdServingRegression_CMP_2026_001Test {" in code
    assert '.param("campaignId", "CMP-2026-001")' in code
    assert "status().isUnprocessableEntity()" in code
    # f-string brace escaping must render single braces in the Java output
    assert "{{" not in code and "}}" not in code
    assert code.count("{") == code.count("}")


@pytest.mark.parametrize(
    "campaign_id, expected",
    [("CMP-1", "CMP_1"), ("cmp.2026/a b", "cmp_2026_a_b"), ("N/A", "Unknown"), ("", "Unknown")],
)
def test_safe_class_suffix_produces_java_identifier(campaign_id, expected):
    assert safe_class_suffix(campaign_id) == expected


def test_junit_test_escapes_quotes_in_campaign_id():
    nasty = 'CMP-"1"\\x'
    code = AIIncidentTriager()._generate_junit_test("AD_BUDGET_EXHAUSTED_ANOMALY", nasty, {})
    assert java_string_literal(nasty) == 'CMP-\\"1\\"\\\\x'
    assert '.param("campaignId", "CMP-\\"1\\"\\\\x")' in code
    assert "public class AdServingRegression_CMP__1__xTest {" in code


def test_triage_incident_returns_rca_and_test(capsys):
    result = AIIncidentTriager().triage_incident(BUDGET_EVENT, {"remaining_budget": 0.0, "active": True})
    assert result["campaign_id"] == "CMP-2026-001"
    assert "ROOT CAUSE ANALYSIS" in result["rca_summary"]
    assert "AdServingRegression_CMP_2026_001Test" in result["junit_test_code"]
    assert "INCIDENT TRIAGE TRIGGERED" in capsys.readouterr().out


def test_triage_incident_handles_missing_fields():
    result = AIIncidentTriager().triage_incident({}, {})
    assert result["campaign_id"] == "N/A"
    assert "UNKNOWN_EVENT" in result["rca_summary"]
    assert "AdServingRegression_UnknownTest" in result["junit_test_code"]


# ---------- File output ----------

def test_save_generated_test_writes_file_to_ad_service_tree(tmp_path, monkeypatch):
    workdir = tmp_path / "ai-agent"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    save_generated_test("// java code", "CMP-7")

    out = tmp_path / "ad-service/src/test/java/com/netflix/ads/generated/AdServingRegression_CMP_7Test.java"
    assert out.read_text(encoding="utf-8") == "// java code"


# ---------- Kafka loop ----------

def test_run_agent_processes_message_end_to_end(monkeypatch):
    message = mock.Mock(value=BUDGET_EVENT)
    monkeypatch.setattr(agent.CockroachDBContextRetriever, "get_campaign_context", lambda self, cid: {"campaign_id": cid})
    saved = mock.Mock()
    monkeypatch.setattr(agent, "save_generated_test", saved)
    # First connection yields one message; the reconnect fails and the retry sleep stops the loop.
    monkeypatch.setattr(agent, "KafkaConsumer", mock.Mock(side_effect=[[message], Exception("broker down")]))
    monkeypatch.setattr(agent.time, "sleep", mock.Mock(side_effect=KeyboardInterrupt))

    with pytest.raises(KeyboardInterrupt):
        agent.run_agent()

    saved.assert_called_once()
    code, campaign_id = saved.call_args.args
    assert campaign_id == "CMP-2026-001"
    assert "AdServingRegression_CMP_2026_001Test" in code


def test_kafka_deserializer_parses_json(monkeypatch):
    consumer = mock.Mock(side_effect=Exception("stop"))
    monkeypatch.setattr(agent, "KafkaConsumer", consumer)
    monkeypatch.setattr(agent.time, "sleep", mock.Mock(side_effect=KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        agent.run_agent()
    deserializer = consumer.call_args.kwargs["value_deserializer"]
    assert deserializer(json.dumps(BUDGET_EVENT).encode("utf-8")) == BUDGET_EVENT
    assert consumer.call_args.args == (agent.KAFKA_TOPIC,)
