import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import random

class MQTTPublisher:
    def __init__(self, client_id):
        self.client = mqtt.Client(client_id)
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        
        # Настройка соединения
        self.client.connect("localhost", 1883, 60)
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("✅ MQTT Publisher connected successfully")
        else:
            print(f"❌ Failed to connect, return code {rc}")
    
    def on_publish(self, client, userdata, mid):
        print(f"📤 Message {mid} published")
    
    def publish_sensor_data(self, device_id, sensor_type, value):
        """Публикация данных с датчика"""
        topic = f"devices/{device_id}/sensors/{sensor_type}"
        
        payload = {
            "device_id": device_id,
            "sensor_type": sensor_type,
            "value": value,
            "timestamp": datetime.now().isoformat(),
            "battery": random.randint(10, 100)
        }
        
        # QoS 1 - гарантированная доставка хотя бы один раз
        result = self.client.publish(
            topic, 
            json.dumps(payload), 
            qos=1,
            retain=True  # Сохраняем последнее значение для новых подписчиков
        )
        
        return result
    
    def publish_alert(self, device_id, alert_type, message, severity="medium"):
        """Публикация алерта"""
        topic = f"alerts/{device_id}/{alert_type}"
        
        payload = {
            "device_id": device_id,
            "alert_type": alert_type,
            "message": message,
            "severity": severity,
            "timestamp": datetime.now().isoformat()
        }
        
        # QoS 2 - гарантированная доставка ровно один раз
        result = self.client.publish(
            topic,
            json.dumps(payload),
            qos=2
        )
        
        return result
    
    def start_publishing(self):
        """Запуск цикла публикации"""
        self.client.loop_start()
        
        devices = [
            {"id": "thermostat_001", "type": "temperature"},
            {"id": "thermostat_001", "type": "humidity"},
            {"id": "motion_001", "type": "motion"}
        ]
        
        try:
            while True:
                for device in devices:
                    # Генерация случайных данных
                    if device["type"] == "temperature":
                        value = round(random.uniform(18.0, 25.0), 1)
                    elif device["type"] == "humidity":
                        value = random.randint(40, 70)
                    else:
                        value = random.choice([0, 1])
                    
                    self.publish_sensor_data(
                        device["id"], 
                        device["type"], 
                        value
                    )
                
                # Случайный алерт
                if random.random() < 0.1:  # 10% chance
                    self.publish_alert(
                        "thermostat_001",
                        "high_temperature",
                        "Temperature above threshold",
                        "high"
                    )
                
                time.sleep(5)  # Публикация каждые 5 секунд
                
        except KeyboardInterrupt:
            print("🛑 Stopping publisher...")
            self.client.loop_stop()
            self.client.disconnect()

if __name__ == "__main__":
    publisher = MQTTPublisher("sensor_publisher_001")
    publisher.start_publishing()