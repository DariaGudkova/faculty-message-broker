"""
Демонстрационный для получения сообщений.
Показывает работу Consumer в реальном времени
"""

from consumer import FacultyConsumer


def start_demo_consumer():
    """
    Запуск consumer в демонстрационном режиме
    """
    print("Демонстрация работы Consumer")
    print("=" * 50)

    # Создаем и запускаем consumer
    consumer = FacultyConsumer()
    consumer.start_consuming()


if __name__ == "__main__":
    start_demo_consumer()
