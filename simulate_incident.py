import json
import time
import requests
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ads.operational.events"
AD_SERVICE_URL = "http://localhost:8081/api/v1/ads"

def simulate_via_kafka():
    print(f"📡 Simulating Direct Kafka Incident Event...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        sample_event = {
            "eventType": "AD_BUDGET_EXHAUSTED_ANOMALY",
            "campaignId": "cmp-netflix-superbowl-2026",
            "advertiser": "Nike Global",
            "remainingBudget": "0.00",
            "active": True,
            "timestamp": int(time.time() * 1000)
        }
        producer.send(KAFKA_TOPIC, key=b"cmp-netflix-superbowl-2026", value=sample_event)
        producer.flush()
        print(f"✅ Published event to Kafka topic '{KAFKA_TOPIC}':")
        print(json.dumps(sample_event, indent=2))
    except Exception as e:
        print(f"❌ Failed to publish to Kafka: {e}")

def simulate_via_http():
    print(f"\n🌐 Simulating HTTP Ad Serving Request to Spring Boot service...")
    try:
        # Step 1: Create Campaign in CockroachDB via REST
        campaign_payload = {
            "campaignId": "cmp-netflix-superbowl-2026",
            "advertiserName": "Nike Global",
            "targetDemographic": "US_MALE_18_34",
            "remainingBudget": 0.00,  # Exhausted budget
            "targetCpm": 25.50,
            "active": True,
            "currentImpressionCount": 50000
        }
        create_res = requests.post(f"{AD_SERVICE_URL}/campaigns", json=campaign_payload)
        print(f"1. Created Campaign in CockroachDB: Status {create_res.status_code}")

        # Step 2: Serve Ad (Triggers Exhaustion Error & Emits Kafka Event)
        serve_res = requests.post(f"{AD_SERVICE_URL}/serve?campaignId=cmp-netflix-superbowl-2026&userDemographic=US_MALE_18_34")
        print(f"2. Serve Ad Request Response: Status {serve_res.status_code}")
        print(f"   Response Body: {serve_res.text}")

    except Exception as e:
        print(f"❌ Failed to communicate with Spring Boot service: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 NETFLIX ADS INCIDENT SIMULATOR")
    print("=" * 60)
    
    # Try Kafka direct simulation
    simulate_via_kafka()
