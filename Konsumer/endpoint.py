from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

def kafka_listener():
    consumer=KafkaConsumer('belajar-topik-lagi',bootstrap_servers=['192.168.0.6:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8'),
                       api_version=(0,10))
    for message in consumer:
        # Split the message by space to separate timestamp and JSON content
        parts = message.value
        return parts
        


@app.route('/data')
def get_data():
    return kafka_listener()


if __name__ == '__main__':
    app.run(debug=True)
