from kafka import KafkaConsumer
import json

# Consumer: anomaly_topic'ten veri almak için
anomaly_consumer = KafkaConsumer(
    'anomaly_topic',  # Sadece anomaly_topic'ten veri al
    bootstrap_servers='localhost:9092',  # Kafka broker adresi
    group_id='anomaly_consumer_group',  # Anomali için ayrı bir grup
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # JSON formatını deserialize et
)

print("Consumer anomaly dinlemede...")
for message in anomaly_consumer:
    data = message.value
    timestamp = data.get('timestamp')
    predicted_gold_price = data.get('predicted_gold_price')
    last_price = data.get('last_price')
    print("Anomali tespit edildi!")
    print(f"Timestamp: {timestamp}")
    print(f"Tahmin edilen fiyat: {predicted_gold_price}")
    print(f"En son fiyat: {last_price}")
    print("-" * 50)
