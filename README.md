# Netflix Ads AI-Triage & Test-Gen Agent

An enterprise-grade reference architecture and working prototype demonstrating an **AI-Native Operational Triage & Automated Test Generation Pipeline** for Netflix Ads Platform microservices.

---

## 🏛️ System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 1. Spring Boot Ad Service                   │
│   • Java 17 + Spring Boot 3                                 │
│   • CockroachDB (Campaign Metadata & Budgets)               │
│   • Kafka Producer (Operational Events & Telemetry)         │
└──────────────────────────────┬──────────────────────────────┘
                               │ Kafka Topic: `ads.operational.events`
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 2. Python AI Agentic Layer                  │
│   • Kafka Consumer & Event Listener                         │
│   • CockroachDB Context Grounding Engine                    │
│   • LLM-Powered Incident Root Cause Analysis (RCA)          │
│   • Automated JUnit 5 / Spring Boot Test Synthesizer        │
└──────────────────────────────┴──────────────────────────────┘
```

---

## 🚀 Key Features

1. **Spring Boot Ads Engine**: Simulates ad serving requests, campaign budget validation, and operational error events.
2. **CockroachDB Integration**: Stores distributed ACID campaign state (budgets, target CPM, active flags).
3. **Kafka Event Bus**: Streams real-time operational telemetry (`AdServingErrorEvent`, `BudgetExhaustedEvent`).
4. **Context-Grounded AI Agent**:
   - Consumes Kafka events in real-time.
   - Queries CockroachDB for live campaign context and database state.
   - Performs automated **Root Cause Analysis (RCA)** on ad delivery anomalies.
   - **Auto-generates a candidate JUnit 5 Spring Boot test** (`AdServingRegressionTest.java`) to prevent future regressions.

---

## 📂 Project Structure

```
netflix-ads-ai-triage/
├── ad-service/                # Java 17 / Spring Boot Microservice
│   ├── build.gradle
│   └── src/main/java/com/netflix/ads/
│       ├── AdsApplication.java
│       ├── controller/AdServingController.java
│       ├── model/AdCampaign.java
│       ├── repository/AdCampaignRepository.java
│       └── service/KafkaEventProducer.java
├── ai-agent/                  # Python Agentic AI Layer
│   ├── requirements.txt
│   ├── agent.py               # Kafka consumer, RCA, and Test Generator
│   └── templates/             # JUnit 5 test prompt templates
├── docker-compose.yml         # CockroachDB & Kafka local cluster
└── README.md
```
