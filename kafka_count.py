# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
from datetime import datetime
import sys
import argparse
import calendar

def count_records(topic, start_time):
    # Thiết lập Kafka Consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',  # Bắt đầu từ offset đầu tiên
        enable_auto_commit=False,  # Tắt chế độ tự động commit offset
        value_deserializer=lambda x: x.decode('utf-8')  # Giải mã giá trị tin nhắn
    )

    # Chuyển đổi thời gian bắt đầu thành timestamp
    start_timestamp = calendar.timegm(start_time.utctimetuple()) * 1000

    # Thiết lập biến đếm
    record_count = 0

    try:
        # Lặp qua các tin nhắn trong topic và đếm số bản ghi từ thời điểm bắt đầu
        for message in consumer:
            if message.timestamp >= start_timestamp:
                record_count += 1
                sys.stdout.write("\rĐang count: {}".format(record_count))
                sys.stdout.flush()
            else:
                # Nếu đến cuối topic hoặc vượt quá thời điểm bắt đầu, thoát khỏi vòng lặp
                break
    except KeyboardInterrupt:
        # Nếu nhận được Ctrl+C, thoát chương trình một cách an toàn
        sys.stdout.write("\rTOTAL: {} messages in topic: {}\n".format(record_count,topic))
        sys.exit(0)

    # In số bản ghi đã đếm được
    print("\nTổng số bản ghi: {}".format(record_count))

if __name__ == "__main__":
    # Thiết lập argparse
    parser = argparse.ArgumentParser(description="Đếm số bản ghi trong Kafka từ thời điểm bắt đầu đến hiện tại.")
    parser.add_argument("--topic", type=str, help="Tên topic Kafka")
    parser.add_argument("--start-time", type=str, default="1900-01-01 00:00:00", help="Thời điểm bắt đầu (YYYY-MM-DD HH:MM:SS)")
    args = parser.parse_args()

    # Chuyển đổi thời gian bắt đầu từ string thành datetime object
    start_time = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")

    # Gọi hàm count_records với các tham số được truyền từ argparse
    count_records(args.topic, start_time)
