"""
Меню для выбора режима работы
"""

import threading
import time
import sys
from demo_producer import send_demo_messages

# Глобальная переменная для управления consumer
consumer_thread = None
consumer_running = False

def show_menu():
    """
    Показать меню выбора режима работы
    """
    print("=" * 60)
    print("СИСТЕМА БРОКЕРА СООБЩЕНИЙ ФАКУЛЬТЕТА")
    print("Выберите режим работы:")
    print("1. Только отправка сообщений (Producer)")
    print("2. Только получение сообщений (Consumer)")
    print("3. Полная демонстрация (Producer и Consumer)")
    print("4. Выход")
    print("=" * 60)

def stop_consumer():
    """
    Корректная остановка consumer
    """
    global consumer_running
    consumer_running = False
    print("Остановка consumer")


def start_consumer_in_thread():
    """
    Запуск consumer в отдельном потоке с возможностью остановки
    """
    global consumer_running
    consumer_running = True

    def consumer_worker():
        from consumer import FacultyConsumer
        consumer = FacultyConsumer()
        consumer.running = consumer_running
        consumer.start_consuming()

    thread = threading.Thread(target=consumer_worker, daemon=True)
    thread.start()
    return thread

def full_demo():
    """
    Полная демонстрация. Одновременная работа Producer и Consumer
    """
    global consumer_thread, consumer_running

    print("Запуск полной демонстрации")

    # Запускаем consumer в отдельном потоке
    consumer_thread = start_consumer_in_thread()

    print("Ожидание запуска consumer")
    time.sleep(2)  # Время для запуска consumer

    # Запускаем producer в основном потоке
    print("Запуск producer")
    send_demo_messages()

    print("\nДемонстрация завершена")
    print("Consumer продолжает работать в фоне")
    print("Для остановки consumer выберите пункт 4 в меню")


def only_consumer():
    """
    Режим только получения сообщений
    """
    global consumer_thread, consumer_running

    if consumer_running:
        print("Consumer уже запущен! Сначала остановите его через пункт 4")
        return

    print("Запуск режима получения сообщений")
    consumer_thread = start_consumer_in_thread()
    print("Consumer запущен и ожидает сообщения")
    print("Для остановки выберите пункт 4 в меню")


def stop_all():
    """
    Остановка всех работающих компонентов
    """
    global consumer_running

    if consumer_running:
        stop_consumer()
        consumer_running = False
        print("Consumer остановлен")
    else:
        print("Consumer не был запущен")

def clean_exit():
    """
    Корректный выход из программы
    """
    print("\nЗавершение работы")
    stop_all()
    print("До свидания!")
    sys.exit(0)

def main():
    """
    Главная функция программы
    """
    global consumer_running

    while True:
        try:
            show_menu()
            choice = input("Ваш выбор (1-4): ").strip()

            if choice == '1':
                #print("\nРежим отправки сообщений")
                send_demo_messages()
                input("\nНажмите Enter для возврата в меню")

            elif choice == '2':
                only_consumer()
                input("\nНажмите Enter для возврата в меню")

            elif choice == '3':
                full_demo()
                input("\nНажмите Enter для возврата в меню")

            elif choice == '4':
                clean_exit()
                break
            else:
                print("Неверный выбор,попробуйте снова")
                input("\nНажмите Enter для продолжения")

        except KeyboardInterrupt:
            print("\n\nОбнаружено прерывание (Ctrl+C)")
            clean_exit()
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            input("\nНажмите Enter для продолжения")


if __name__ == "__main__":
    main()