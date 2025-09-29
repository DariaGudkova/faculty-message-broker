"""
Модуль для отправки сообщений в Kafka.
Отвечает за создание и отправку сообщений в брокер
"""

import json
import logging
from datetime import datetime
from confluent_kafka import Producer
from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, MESSAGE_TYPES

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('FacultyProducer')


class FacultyProducer:
    """
    Класс для отправки сообщений в систему Kafka
    """

    def __init__(self):
        """Инициализация подключения к Kafka"""
        # Настройки для подключения к Kafka серверу
        self.conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,  # Адрес сервера
            'client.id': 'faculty-producer'  # Идентификатор клиента
        }

        # Создаем producer
        self.producer = Producer(self.conf)
        self.topic = KAFKA_TOPIC  # Топик, в который отправляем сообщения

        logger.info(f"Producer подключен к Kafka на {KAFKA_BOOTSTRAP_SERVERS}")

    def delivery_callback(self, err, msg):
        """
        Функция обратного вызова. Вызывается когда сообщение доставлено или произошла ошибка доставки
        """
        if err is not None:
            # Если ошибка, логируем ее
            logger.error(f"Ошибка доставки сообщения: {err}")
        else:
            # Если успешно, подтверждаем доставку
            logger.info(f"Сообщение доставлено в топик {msg.topic()}")

    def send_message(self, message_type, data):
        """
        Основной метод для отправки сообщения
        """
        try:
            # Создаем структурированное сообщение
            message = {
                'type': message_type,
                'data': data,
                'timestamp': datetime.now().isoformat(),  # Время создания
                'version': '1.0'
            }

            # Преобразуем в JSON и отправляем в Kafka
            self.producer.produce(
                topic=self.topic,  # Куда отправляем
                value=json.dumps(message).encode('utf-8'),  # Что отправляем
                callback=self.delivery_callback  # Что вызвать после отправки
            )

            # Обрабатываем callback
            self.producer.poll(0)
            logger.info(f"Отправлено сообщение типа: {message_type}")

        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения: {e}")

    def send_grade_update(self, student_id, student_name, course, grade, professor):
        """
        Метод для отправки уведомления об оценке
        """
        data = {
            'student_id': student_id,
            'student_name': student_name,
            'course': course,
            'grade': grade,
            'professor': professor,
            'date': datetime.now().strftime('%Y-%m-%d')
        }
        self.send_message(MESSAGE_TYPES['GRADE_UPDATE'], data)

    def send_schedule_change(self, subject, old_time, new_time, classroom, comment):
        """
        Метод для отправки изменения расписания
        """
        data = {
            'subject': subject,
            'old_time': old_time,
            'new_time': new_time,
            'classroom': classroom,
            'comment': comment
        }
        self.send_message(MESSAGE_TYPES['SCHEDULE_CHANGE'], data)

    def flush(self):
        """
        Принудительная отправка всех сообщений из буфера (Вызывать этот метод перед завершением программы)
        """
        self.producer.flush()
        logger.info("Все сообщения отправлены")


if __name__ == "__main__":
    # Создаем producer
    producer = FacultyProducer()

    # Тестовое сообщение
    producer.send_grade_update(
        student_id="16220223",
        student_name="Иванов Иван",
        course="Математический анализ",
        grade="5 (Отлично)",
        professor="Петров П.П."
    )

    producer.flush()
