import subprocess
import time
from psycopg2 import sql
import os
import logging
from datetime import datetime

# Путь к виртуальному окружению
VENV_PATH = '/home/ubuntu/scripts/venv/bin/python3'
# Параметры подключения к базе данных PostgreSQL

# Настройка логирования
log_directory = "/home/ubuntu/scripts/mia/posts/log/"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logging.basicConfig(
    filename=os.path.join(log_directory, f"cron_posts_{datetime.now().strftime('%Y-%m-%d')}.log"),
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

# Основной цикл
def main():
    if run_script('/home/ubuntu/scripts/mia/posts/1-posts_gen-llama_sonar31-sm.py'):
        # last_id_after = get_last_news_id()
        # print(f"Последний ID после запуска скрипта: {last_id_after}")
        # logging.info(f"Последний ID после запуска скриптов: {last_id_after}")

        if run_script('/home/ubuntu/scripts/mia/posts/2_loc_wp_blog-posts_sync_o1.py'):
                logging.info("Цикл завершен. Следующий запуск через 24 часа. Запуск из CRONTAB")
            # print("Обнаружены новые записи. Продолжаем выполнение скриптов.")
            # logging.info("Обнаружены новые записи. Продолжаем выполнение скриптов.")
        else:
            print("Ошибка при выполнении 2_loc_wp_blog-posts_sync_o1.py")
            logging.error("Ошибка при выполнении 2-post_blog_sync.py")
    else:
        print("Ошибка при выполнении 1-posts_gen-llama_sonar31-sm.py")
        logging.error("Ошибка при выполнении 1-posts_gen-llama_sonar31-sm.py")

if __name__ == "__main__":
    main()