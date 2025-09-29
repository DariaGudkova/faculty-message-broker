"""
Модуль для получения сообщений из Kafka.
Слушает (читает) топик и обрабатывает входящие сообщения
"""

import json
import logging
import time
from confluent_kafka import Consumer, KafkaError
from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID, MESSAGE_TYPES

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('FacultyConsumer')


class FacultyConsumer:
    """
    Класс для получения и обработки сообщений из Kafka
    """

    def __init__(self):
        """Инициализация consumer"""
        # Настройки для consumer'а
        self.conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }

        self.consumer = Consumer(self.conf)
        self.topic = KAFKA_TOPIC
        self.running = False  # Флаг для управления работой consumer

        logger.info("Consumer инициализирован")

    def handle_grade_update(self, data):
        """
        Обработчик сообщений об обновлении оценок
        """
        logger.info(f"Обработка новой оценки для студента {data['student_name']}")

        print("\n" + "=" * 50)
        print("НОВАЯ ОЦЕНКА")
        print(f"Студент: {data['student_name']} ({data['student_id']})")
        print(f"Дисциплина: {data['course']}")
        print(f"Оценка: {data['grade']}")
        print(f"Преподаватель: {data['professor']}")
        print(f"Дата: {data['date']}")
        print("=" * 50)

    def handle_schedule_change(self, data):
        """
        Обработчик сообщений об изменении расписания
        """
        logger.info(f"Обработка изменения расписания для {data['subject']}")

        print("\n" + "=" * 50)
        print("ИЗМЕНЕНИЕ РАСПИСАНИЯ")
        print(f"Дисциплина: {data['subject']}")
        print(f"Старое время: {data['old_time']}")
        print(f"Новое время: {data['new_time']}")
        print(f"Аудитория: {data['classroom']}")
        print(f"Комментарий: {data['comment']}")
        print("=" * 50)

    def handle_new_announcement(self, data):
        """
        Обработчик новых объявлений
        """
        logger.info(f"Обработка нового объявления: {data['title']}")

        print("\n" + "=" * 50)
        print("НОВОЕ ОБЪЯВЛЕНИЕ")
        print(f"Заголовок: {data['title']}")
        print(f"Текст: {data['message']}")
        print(f"Автор: {data['author']}")
        print("=" * 50)

    def handle_exam_notification(self, data):
        """
        Обработчик уведомлений об экзаменах
        """
        logger.info(f"Обработка уведомления об экзамене: {data['subject']}")

        print("\n" + "=" * 50)
        print("УВЕДОМЛЕНИЕ ОБ ЭКЗАМЕНЕ")
        print(f"Дисциплина: {data['subject']}")
        print(f"Дата: {data['date']}")
        print(f"Время: {data['time']}")
        print(f"Аудитория: {data['classroom']}")
        print(f"Преподаватель: {data['professor']}")
        print("=" * 50)

    def process_message(self, message):
        """
        Основной метод обработки входящего сообщения.
        Определяет тип сообщения и вызывает соответствующий обработчик
        """
        try:
            message_type = message['type']
            data = message['data']

            logger.info(f"Получено сообщение типа: {message_type}")

            if message_type == MESSAGE_TYPES['GRADE_UPDATE']:
                self.handle_grade_update(data)
            elif message_type == MESSAGE_TYPES['SCHEDULE_CHANGE']:
                self.handle_schedule_change(data)
            elif message_type == MESSAGE_TYPES['NEW_ANNOUNCEMENT']:
                self.handle_new_announcement(data)
            elif message_type == MESSAGE_TYPES['EXAM_NOTIFICATION']:
                self.handle_exam_notification(data)
            else:
                logger.warning(f"Неизвестный тип сообщения: {message_type}")
                print(f"Неизвестный тип сообщения: {message_type}")

        except KeyError as e:
            logger.error(f"Отсутствует поле в сообщении: {e}")
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")

    def start_consuming(self):
        """
        Запуск чтения сообщений с проверкой флага running
        """
        # Подписываемся на топик
        self.consumer.subscribe([self.topic])
        self.running = True

        logger.info(f"Начало чтение топика: {self.topic}")
        print("Consumer запущен и готов к работе")
        print("Ожидание сообщений")
        print("Consumer будет остановлен при выборе пункта 4 в меню\n")

        try:
            # Основной цикл обработки сообщений
            while self.running:
                # Получаем сообщение (ждем макс 1 сек)
                msg = self.consumer.poll(0.5)

                if msg is None:
                    continue  # Сообщений нет, ждём дальше

                # Проверяем ошибки
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # конец раздела
                        continue
                    else:
                        logger.error(f"Ошибка Kafka: {msg.error()}")
                        break

                try:
                    # Декодируем JSON сообщение
                    message_json = msg.value().decode('utf-8')
                    message = json.loads(message_json)

                    # Обрабатываем сообщение
                    self.process_message(message)

                    # Подтверждаем обработку сообщения
                    self.consumer.commit(asynchronous=False)

                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка декодирования JSON: {e}")
                except Exception as e:
                    logger.error(f"Ошибка обработки: {e}")

        except KeyboardInterrupt:
            print("\nПолучена команда остановки")
        finally:
            # Закрываем consumer
            self.stop()

    def stop(self):
        """
        Корректная остановка consumer
        """
        self.running = False
        self.consumer.close()
        logger.info("Consumer остановлен")
        print("Consumer корректно остановлен")


if __name__ == "__main__":
    consumer = FacultyConsumer()

    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("\nЗавершение работы consumer")