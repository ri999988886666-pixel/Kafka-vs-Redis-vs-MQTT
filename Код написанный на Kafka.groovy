from kafka import KafkaProducer
import json
import time
from datetime import datetime

class OrderProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
    
    def send_order_event(self, order_data):
        """Отправка события о заказе"""
        event = {
            **order_data,
            "timestamp": datetime.now().isoformat(),
            "event_id": f"event_{int(time.time())}"
        }
        
        # Отправка в топик заказов с ключом по user_id для гарантированного порядка
        future = self.producer.send(
            'order-events',
            key=order_data['user_id'].encode('utf-8'),
            value=event
        )
        
        # Ожидание подтверждения
        try:
            future.get(timeout=10)
            print(f"✅ Order event sent: {order_data['order_id']}")
        except Exception as e:
            print(f"❌ Failed to send event: {e}")
    
    def close(self):
        self.producer.close()

# Использование
if __name__ == "__main__":
    producer = OrderProducer()
    
    orders = [
        {
            "order_id": "order_001",
            "user_id": "user_123",
            "status": "created",
            "amount": 1500.50,
            "items": ["laptop", "mouse"]
        },
        {
            "order_id": "order_002", 
            "user_id": "user_456",
            "status": "paid",
            "amount": 299.99,
            "items": ["book", "pen"]
        }
    ]
    
    for order in orders:
        producer.send_order_event(order)
        time.sleep(1)
    
    producer.close()