"""
Константы для модуля брокера сообщений факультета
Все настройки собраны в одном месте для простоты изменения
"""

# Настройки подключения к Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Адрес Kafka сервера
KAFKA_TOPIC = 'faculty_notifications'       # Название топика для сообщений
KAFKA_GROUP_ID = 'faculty-consumer-group'   # ID группы для consumer'ов

# Типы сообщений, которые поддерживает система
MESSAGE_TYPES = {
    'GRADE_UPDATE': 'grade_update',          # Обновление оценки
    'SCHEDULE_CHANGE': 'schedule_change',    # Изменение расписания
    'NEW_ANNOUNCEMENT': 'new_announcement',  # Новое объявление
    'EXAM_NOTIFICATION': 'exam_notification' # Уведомление об экзамене
}

# Настройки логирования
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'
