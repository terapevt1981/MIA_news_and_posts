import subprocess
import time
import psycopg2
from psycopg2 import sql
import os
import logging
from datetime import datetime

# Путь к виртуальному окружению
VENV_PATH = '/home/ubuntu/scripts/venv/bin/python'
# Параметры подключения к базе данных PostgreSQL
DB_PARAMS = {
    'dbname': 'miatennispro',
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': '5432',
    'sslmode': 'require'  
}

# Настройка логирования
log_directory = "/home/ubuntu/scripts/mia/news/log/"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logging.basicConfig(
    filename=os.path.join(log_directory, f"cron_news_{datetime.now().strftime('%Y-%m-%d')}.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Функция для выполнения скрипта и ожидания его завершения
def run_script(script_name):
    try:
        logging.info(f"Запуск скрипта: {script_name}")
        result = subprocess.run([VENV_PATH, script_name], check=True, capture_output=True, text=True)
        print(f"{script_name} завершен успешно.")
        logging.info(f"{script_name} завершен успешно.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении {script_name}: {e.stderr}")
        logging.error(f"Ошибка при выполнении {script_name}: {e.stderr}")
        return False

# Функция для получения последнего id в таблице news
def get_last_news_id():
    # Добавьте перед подключением к базе данных
    # logging.info(f"DB_USERNAME: {os.getenv('DB_USERNAME')}, DB_PASSWORD: {os.getenv('DB_PASSWORD')}, DB_HOST: {os.getenv('DB_HOST')}")

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM news ORDER BY id DESC LIMIT 1")
        last_id = cursor.fetchone()
        conn.close()
        logging.info(f"Получен последний ID из таблицы news: {last_id[0] if last_id else 'Нет записей'}")
        return last_id[0] if last_id else None
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        logging.error(f"Ошибка при подключении к базе данных: {e}")
        return None

# Основной цикл
def main():
    last_id_before = get_last_news_id()
    if last_id_before is None:
        print("Не удалось получить последний ID. Скрипты не запущены.")
        logging.error("Не удалось получить последний ID. Скрипты не запущены.")
        return

    logging.info(f"Последний ID перед запуском скрипта: {last_id_before}")

    if run_script('/home/ubuntu/scripts/mia/news/1-rss+generate_perplex.py'):
        last_id_after = get_last_news_id()
        print(f"Последний ID после запуска скрипта: {last_id_after}")
        logging.info(f"Последний ID после запуска скриптов: {last_id_after}")

        if last_id_after and last_id_after > last_id_before:
            print("Обнаружены новые записи. Продолжаем выполнение скриптов.")
            logging.info("Обнаружены новые записи. Продолжаем выполнение скриптов.")
            if run_script('/home/ubuntu/scripts/mia/news/2_loc_wp_news_sync_o1.py'):
                logging.info("Цикл завершен. Ожидание 5 минут перед следующим циклом. Запуск из CRONTAB")
            else:
                print("Ошибка при выполнении 2_loc_wp_news_sync_o1.py")
                logging.error("Ошибка при выполнении 2_loc_wp_news_sync_o1.py")
                
        else:
            print("Новые записи не обнаружены. Скрипты не запущены.")
            logging.error("Новые записи не обнаружены. Скрипты не запущены.")
    else:
        print("Ошибка при выполнении 1-rss_db_update.py")
        logging.error("Ошибка при выполнении 1-rss_db_update.py")

if __name__ == "__main__":
    main()