from kafka import KafkaConsumer
import json

# Consumer: normal_topic'ten veri almak için
normal_consumer = KafkaConsumer(
    'normal_topic',  # Sadece normal_topic'ten veri al
    bootstrap_servers='localhost:9092',  # Kafka broker adresi
    group_id='normal_consumer_group',  # Normal veri için ayrı bir grup
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # JSON formatını deserialize et
)

print("Consumer normal dinlemede...")
for message in normal_consumer:
    data = message.value
    timestamp = data.get('timestamp')
    predicted_gold_price = data.get('predicted_gold_price')
    last_price = data.get('last_price')
    print("Normal veri")
    print(f"Timestamp: {timestamp}")
    print(f"Tahmin edilen fiyat: {predicted_gold_price}")
    print(f"En son fiyat: {last_price}")
    print("-" * 50)
