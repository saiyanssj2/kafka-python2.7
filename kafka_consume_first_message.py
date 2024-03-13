# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
import argparse

def print_first_message(topic):
    # Thiết lập Kafka Consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',  # Bắt đầu từ offset đầu tiên
        enable_auto_commit=False,  # Tắt chế độ tự động commit offset
        value_deserializer=lambda x: x.decode('utf-8')  # Giải mã giá trị tin nhắn
    )

    # Lấy tin nhắn đầu tiên từ topic và in ra
    message = next(consumer)
    print(message.value)

if __name__ == "__main__":
    # Thiết lập argparse
    parser = argparse.ArgumentParser(description="In ra tin nhắn đầu tiên của topic Kafka được chỉ định.")
    parser.add_argument("--topic", type=str, help="Tên topic Kafka")
    args = parser.parse_args()

    # Gọi hàm print_first_message với topic được truyền từ argparse
    print_first_message(args.topic)