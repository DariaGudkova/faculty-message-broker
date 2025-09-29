"""
Демонстрационный для отправки тестовых сообщений.
Показывает как использовать Producer на практике
"""

from producer import FacultyProducer
from constants import MESSAGE_TYPES
from datetime import datetime, timedelta


def send_demo_messages():
    """
    Отправка демонстрационных сообщений разных типов
    """
    print("Запуск демонстрации отправки сообщений")

    # Создаем producer
    producer = FacultyProducer()

    print("\n1. Отправка уведомления об оценке")
    # Пример 1 уведомление об оценке
    producer.send_grade_update(
        student_id="16220223",
        student_name="Иванов Иван",
        course="Математический анализ",
        grade="5 (Отлично)",
        professor="Петров П.П."
    )

    print("2. Отправка изменения расписания")
    # Пример 2 изменение расписания
    producer.send_schedule_change(
        subject="Базы данных",
        old_time="2025-09-03 09:45",
        new_time="2025-09-03 15:10",
        classroom="295",
        comment="-"
    )

    print("3. Отправка нового объявления")
    # Пример 3 новое объявление
    producer.send_message(MESSAGE_TYPES['NEW_ANNOUNCEMENT'], {
        'title': 'Важная информация',
        'message': 'Занятия 10 сентября переносятся в дистанционный режим в связи с проведением научной конференции',
        'author': 'Деканат факультета',
        'priority': 'high'
    })

    print("4. Отправка уведомления об экзамене")
    # Пример 4 уведомление об экзамене
    producer.send_message(MESSAGE_TYPES['EXAM_NOTIFICATION'], {
        'subject': 'Базы данных',
        'date': '2025-06-10',
        'time': '10:00',
        'classroom': '381',
        'professor': 'Петров П.П.'
    })

    # Отправляем все сообщения
    producer.flush()
    print("\nВсе демонстрационные сообщения отправлены!")
    # print("Проверьте consumer для просмотра полученных сообщений")


if __name__ == "__main__":
    send_demo_messages()
