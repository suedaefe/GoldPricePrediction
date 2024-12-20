from kafka import KafkaProducer
import json
import time
from keras.api import *
from keras.api.models import load_model
import numpy as np
import pandas as pd

# Kafka Producer Configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Modeli yükle
model = load_model('C:\\Users\\sueda\\Masaüstü\\bigdata\\gold_price_model.h5')

# Örnek geçmiş veriyi yükle 
historical_data = pd.read_csv("C:\\Users\\sueda\\Masaüstü\\bigdata\\goldprice2023.csv")  # Altın fiyatları veri seti

# 'Vol.' sütunundaki NaN değerlerini temizle
historical_data = historical_data.dropna(subset=['Vol.'])

# 'Price', 'Open', 'High', 'Low' ve 'Change %' sütunlarını sayısal değerlere dönüştür
historical_data['Price'] = historical_data['Price'].str.replace(',', '').astype(float)
historical_data['Open'] = historical_data['Open'].str.replace(',', '').astype(float)
historical_data['High'] = historical_data['High'].str.replace(',', '').astype(float)
historical_data['Low'] = historical_data['Low'].str.replace(',', '').astype(float)
historical_data['Change %'] = historical_data['Change %'].str.replace('%', '').astype(float)

# 'Date' sütununu tarih formatına dönüştür
historical_data['Date'] = pd.to_datetime(historical_data['Date'], format='%m/%d/%Y')

# Temizlenmiş veri setini kontrol et
print(historical_data.head())

# Eğer veri setinde boş değerler varsa, kontrol edin
if historical_data.empty:
    print("Veri seti boş, işlem yapılamaz!")

# Son altın fiyatını al
if not historical_data.empty:
    last_data = historical_data['Price'].iloc[-1]  # Son altın fiyatı
else:
    print("Veri seti boş, işlem yapılamaz!")

# 'Price' sütunundaki sayısal olmayan değerleri temizle
historical_data['Price'] = pd.to_numeric(historical_data['Price'], errors='coerce')  # Sayısal olmayanları NaN yap
historical_data = historical_data.dropna(subset=['Price'])  # NaN olan satırları temizle

# Model tahminini normalizasyonu kaldırarak gerçek fiyata dönüştür
def generate_data(last_data):
    """
    Geçmiş verilerle model tahmini yapma.
    last_data: son tarihli altın fiyatı (modelin giriş verisi)
    """
    # Modelin giriş formatına uygun hale getir
    input_data = np.array([last_data]).reshape((1, 1, 1))  # Modelin beklediği 3D formata dönüştür
    
    # Modeli kullanarak tahmin yap
    predicted_price = model.predict(input_data)
    
    # Normalize edilmiş veriyi geri dönüştür
    max_price = 2000  # Eğitim sırasında kullanılan maksimum değer yaklaşık
    predicted_price_real = predicted_price[0][0] * max_price
    
    return predicted_price_real  # Gerçek altın fiyatı

# Anomali tespiti için eşik
def is_anomaly(predicted_price, last_price, threshold=0.1):
    price_diff = abs(predicted_price - last_price)
    if price_diff / last_price > threshold:
        return True
    return False

def send_data():
    last_data = historical_data['Price'].iloc[-1]  # Son altın fiyatı

    while True:
        # Model kullanarak yeni altın fiyatı tahmin et
        predicted_price = generate_data(last_data)

        # predicted_price ve last_data'yı float'a dönüştür
        predicted_price = float(predicted_price)  # float32'yi float'a dönüştür
        last_data = float(last_data)  # np.float64'yi float'a dönüştür

        # Anomali tespiti yap
        if is_anomaly(predicted_price, last_data):
            # Anomali tespit edilirse anomaly_topic'e gönder
            topic = 'anomaly_topic'
        else:
            # Normal veri ise normal_topic'e gönder
            topic = 'normal_topic'

        new_data = {
            'timestamp': time.time(),
            'predicted_gold_price': predicted_price,  # Modelin tahmin ettiği altın fiyatı
            'last_price': last_data,  # Son fiyat
        }
        
        # Veriyi Kafka topic'ine gönder
        producer.send(topic, new_data)
        print(f"Veri gönderildi: {new_data} topic : {topic}")

        # Geçmiş veriyi güncelle (modelin aldığı son veri)
        last_data = predicted_price  # Yeni tahmin, bir sonraki günün tahminine giriş olacak

        time.sleep(60)  # Günlük veri gönderimi için gecikme

send_data()
