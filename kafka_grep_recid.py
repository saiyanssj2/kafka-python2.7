# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
import argparse
import sys

def print_messages_by_recid(topic, recid):
    # Thiết lập Kafka Consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',  # Bắt đầu từ offset đầu tiên
        enable_auto_commit=False,  # Tắt chế độ tự động commit offset
        value_deserializer=lambda x: x.decode('utf-8')  # Giải mã giá trị tin nhắn
    )

    # Thiết lập biến đếm
    record_count = 0
    strRECID = '"RECID":"' + recid + '"'
    # Lặp qua các tin nhắn trong topic và in ra những tin nhắn có RECID mong muốn
    try:
        for message in consumer:
            if strRECID in message.value:
                print
                print(message.value)
            record_count += 1
            sys.stdout.write("\rĐang count: {}".format(record_count))
            sys.stdout.flush()
    except KeyboardInterrupt:
        # Nếu nhận được Ctrl+C, thoát chương trình một cách an toàn
        sys.stdout.write("\rTOTAL: {} messages in topic: {}\n".format(record_count,topic))
        sys.exit(0)

if __name__ == "__main__":
    # Thiết lập argparse
    parser = argparse.ArgumentParser(description="In ra tất cả các tin nhắn trong Kafka topic có RECID như mong muốn.")
    parser.add_argument("--topic", type=str, help="Tên topic Kafka")
    parser.add_argument("--recid", type=str, help="Giá trị RECID cần tìm")
    args = parser.parse_args()

    # Gọi hàm print_messages_by_recid với các tham số được truyền từ argparse
    print_messages_by_recid(args.topic, args.recid)
