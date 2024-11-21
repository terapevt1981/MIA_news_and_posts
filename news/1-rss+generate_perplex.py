import cloudscraper  # Добавляем импорт cloudscraper
import requests
import logging
import re
import os
import psycopg2
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
import feedparser
from dateutil import parser
import time
from dateutil import tz
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from threading import Lock

# Словарь для сопоставления временных зон
tzinfos = {
    "EST": tz.gettz("America/New_York"),
    "EDT": tz.gettz("America/New_York"),
    "CST": tz.gettz("America/Chicago"),
    "CDT": tz.gettz("America/Chicago"),
    "PST": tz.gettz("America/Los_Angeles"),
    "PDT": tz.gettz("America/Los_Angeles"),
    "MST": tz.gettz("America/Denver"),
    "MDT": tz.gettz("America/Denver")
}

# Настройка логирования в файл
log_directory = "/home/ubuntu/scripts/mia/news/log/"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
    
current_date = datetime.now().strftime('%Y-%m-%d')
log_file = os.path.join(log_directory, f'1-rss+perplexity_{current_date}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_file,
    filemode='a'
)

# Переменные для API и базы данных
API_KEY = os.getenv('API_KEY_PERPLEXITY')
PERPLEXITY_API_URL = 'https://api.perplexity.ai/chat/completions'

# Подключение к базе данных PostgreSQL
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database="miatennispro",
            port="5432",
            user=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD')
        )
        logging.info("Подключение к базе данных успешно.")
        return conn
    except Exception as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        return None

# Остальные функции без изменений, если они не требуют модификаций


# Получение последней даты публикации из базы данных
def get_last_pub_date(conn, use_custom_date=False, days_back=2):
    cursor = conn.cursor()
    if use_custom_date:
        return datetime.now() - timedelta(days=days_back)
    else:
        cursor.execute("SELECT MAX(pub_date) FROM news")
        return cursor.fetchone()[0] or datetime.now() - timedelta(days=days_back)

# Проверка на существование новости в базе данных
def check_news_in_db(conn, news_url):
    try:
        cursor = conn.cursor()
        query = "SELECT id FROM news WHERE source_url = %s"
        cursor.execute(query, (news_url,))
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        logging.error(f"Ошибка при проверке новости в БД: {e}")
        return False

# Функция для сохранения информации об изображениях в базе данных
def save_image_info(conn, post_id, image_url, alt_text):
    logging.debug(f"Сохраняем информацию об изображении для поста ID {post_id}.")
    query = """
    INSERT INTO post_images (post_id, image_url, alt_text)
    VALUES (%s, %s, %s)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (post_id, image_url, alt_text))
        conn.commit()
        logging.debug(f"Информация об изображении успешно сохранена в БД для поста ID {post_id}.")
    except Exception as e:
        logging.error(f"Ошибка при сохранении информации об изображении: {e}")

# Функция для скрапинга контента и изображений с помощью Playwright
def scrape_content_with_playwright(news_url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            logging.info(f"Начинаем скрапинг контента с URL: {news_url}")

            # Открываем новость
            page.goto(news_url, timeout=10000)  # Увеличен тайм-аут до 30000 = 30 секунд

            # Попытка "принять куки"
            try:
                page.click("text=Accept Cookies")
                logging.info(f"Нажата кнопка 'Принять куки'")
            except:
                logging.debug("Кнопка 'Принять куки' не найдена или уже принята.")

            # Извлечение текста статьи
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')

            # Попытка найти основной текст статьи
            article = soup.find('article')
            if article:
                article_content = article.get_text(separator="\n").strip()
            else:
                # Если тег <article> не найден, берем весь текст
                article_content = soup.get_text(separator="\n").strip()

            # Извлечение изображений и их alt-текстов
            images = []
            image_elements = soup.find_all('img')
            for img in image_elements:
                src = img.get('src')
                alt = img.get('alt', '')
                if src:
                    images.append((src, alt))

            logging.debug(f"Скрапинг завершён. Длина контента: {len(article_content)}, количество изображений: {len(images)}")

            browser.close()
            return article_content, images
    except Exception as e:
        logging.error(f"Ошибка при скрапинге контента с помощью Playwright: {e}")
        return None, []

# Функция для сохранения поста и информации об изображениях
# Функция для сохранения новости в таблицу "news" на первом этапе и информации об изображениях
def save_news_to_db(conn, post_data, source_url, news_id, images):
    logging.debug(f"Сохраняем новость {news_id} в таблицу 'posts'.")

    # Проверка адекватности данных перед сохранением
    title = post_data.get('title', '')
    content = post_data.get('content', '')

    # Если длина title менее 20 символов или длина content менее 50 символов, установим статус 'NO'
    if len(title) < 20 or len(content) < 50:
        post_data['status'] = 'NO'
    else:
        post_data['status'] = post_data.get('status', 'pre-Draft')

    # SQL запрос для вставки данных в таблицу posts
    query = """
    INSERT INTO posts (
        title, content, source_url, status, tags, pub_date, category_id, category_name, news_id, seo_title, seo_metadesc, seo_focuskw, seo_slug
    ) VALUES (
        %(title)s, %(content)s, %(source_url)s, %(status)s, %(tags)s, %(pub_date)s, %(category_id)s, %(category_name)s, %(news_id)s, %(seo_title)s, %(seo_metadesc)s, %(seo_focuskw)s, %(seo_slug)s
    ) RETURNING id
    """
    try:
        cursor = conn.cursor()

        # Убедимся, что все нужные данные присутствуют
        post_data['status'] = post_data.get('status', 'pre-Draft')
        post_data['pub_date'] = post_data.get('pub_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        post_data['source_url'] = source_url if source_url else post_data.get('source_url')
        post_data['news_id'] = news_id  # ID новости
        post_data['category_id'] = post_data.get('category_id', 8)
        post_data['category_name'] = post_data.get('category_name', 'News')

        # Выполняем SQL-запрос
        cursor.execute(query, post_data)
        post_id = cursor.fetchone()[0]
        conn.commit()
        logging.debug(f"Новость успешно сохранена в таблицу 'posts' с ID {post_id}.")

        # Сохранение информации об изображениях
        for image_url, alt_text in images:
            save_image_info(conn, post_id, image_url, alt_text)

        return post_id
    except Exception as e:
        logging.error(f"Ошибка при сохранении новости в таблицу 'posts': {e}")
        return None

# Функция для получения новостей, которые не были отправлены на обработку модели.
def get_unprocessed_news(conn):
    """
    Функция для получения новостей, которые не были отправлены на обработку модели.
    Выбираем новости, созданные не более 2 дней назад, которых нет в таблице posts, и проверяем что tested != 2.
    """
    try:
        cursor = conn.cursor()
        query = """
        SELECT id, title, source_url, pub_date, content
        FROM news
        WHERE id NOT IN (SELECT news_id FROM posts)
        AND tested != 2
        AND pub_date >= NOW() - INTERVAL '2 days';  -- Фильтр по дате
        """
        cursor.execute(query)
        unprocessed_news = cursor.fetchall()
        return unprocessed_news
    except Exception as e:
        logging.error(f"Ошибка при получении необработанных новостей: {e}")
        return []

# Обработка всех новостей, которые не были отправлены модели на генерацию контента.
def process_unprocessed_news(conn):
    """
    Обработка всех новостей, которые не были отправлены модели на генерацию контента.
    """
    unprocessed_news = get_unprocessed_news(conn)
    
    if not unprocessed_news:
        logging.info("Нет новостей для обработки.")
        return
    
    for news in unprocessed_news:
        news_id, title, source_url, pub_date, content = news
        logging.info(f"Обрабатываем новость с ID: {news_id}, заголовок: {title}")
        
        # Извлечение контента и изображений с помощью Playwright
        scraped_content, images = scrape_content_with_playwright(source_url)
        logging.debug(f"Извлеченный контент длиной {len(scraped_content) if scraped_content else 0} символов.")
        logging.debug(f"Количество извлеченных изображений: {len(images)}")
        
        if not scraped_content:
            logging.error(f"Не удалось скрапить контент для новости ID {news_id}. Пропуск.")
            update_news_status(conn, news_id, tested_value=2)
            continue  # Переход к следующей новости
        
        # Вызов функции с необходимыми аргументами, включая images
        status, result = fetch_news_from_perplexity(source_url, title, pub_date, scraped_content, images)
        
        if status == 'valid' and result:
            # Логируем весь результат перед дальнейшей обработкой
            logging.debug(f"Полный ответ от API Perplexity: {result}")
            
            # Логируем конкретную часть ответа, которую передаем в extract_post_data
            try:
                generated_content = result['choices'][0]['message']['content']
                # logging.debug(f"Передаем следующие данные в extract_post_data: {generated_content}")
                
                post_data = extract_post_data(generated_content)
                
                if post_data:
                    save_news_to_db(conn, post_data, source_url, news_id, images)  # Передаем images
                    update_news_status(conn, news_id, tested_value=1)  # Успешная обработка
                else:
                    logging.error(f"Не удалось извлечь данные для новости ID: {news_id}")
            except (KeyError, IndexError) as e:
                logging.error(f"Ошибка при доступе к 'choices' или 'message': {e}. Полный ответ: {result}")
                
        elif status == 'failed_to_download':
            logging.info(f"Не удалось скачать данные для новости ID: {news_id}, URL: {source_url}")
            update_news_status(conn, news_id, tested_value=2)  # Обновляем как необработанную
        elif status == 'not_tennis_news':
            logging.info(f"Новость ID: {news_id} не относится к теннису. Пропуск.")
            update_news_status(conn, news_id, tested_value=2)  # Обновляем как нерелевантную

# Функция для сохранения данных поста в базу данных
def save_news_in_db(conn, news_data):
    logging.debug(f"Сохраняем новость в таблицу 'news': {news_data['title']}")

    query = """
    INSERT INTO news (title, content, source_url, pub_date, tags)
    VALUES (%(title)s, %(content)s, %(link)s, %(pub_date)s, %(tags)s)
    RETURNING id
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, news_data)
        news_id = cursor.fetchone()[0]
        conn.commit()
        return news_id
    except Exception as e:
        logging.error(f"Ошибка при сохранении новости в таблицу 'news': {e}")
        return None

# Функция для обновления статуса записи в таблице "news"
def update_news_status(conn, news_id, tested_value):
    logging.debug(f"Обновляем статус новости с ID {news_id} в таблице 'news'.")
    
    query = """
    UPDATE news
    SET tested = %(tested)s
    WHERE id = %(news_id)s;
    """
    try:
        cursor = conn.cursor()
        data = {
            'tested': tested_value,
            'news_id': news_id
        }
        cursor.execute(query, data)
        conn.commit()
        logging.debug(f"Статус новости с ID {news_id} успешно обновлен.")
    except Exception as e:
        logging.error(f"Ошибка при обновлении статуса новости в таблице 'news': {e}")

# Функция для извлечения данных поста (аналог extract_seo_data)
def extract_post_data(final_content):
    # Проверим, что final_content — это строка
    if not isinstance(final_content, str):
        logging.error(f"Ожидалась строка, но получен объект типа {type(final_content)}: {final_content}")
        return None
    
    data = {}
    try:
        # Регулярные выражения с учетом возможных переносов строк и пробелов
        title_match = re.search(r'\$\$[tT]itle\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
        content_match = re.search(r'\$\$[cC]ontent\$\$\s*[:\-]*\s*(.*?)\s*(?=\n*\$\$|$)', final_content, re.DOTALL)
        tags_match = re.search(r'\$\$[tT]ags\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
        seo_title_match = re.search(r'\$\$[sS]EO [tT]itle\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
        focus_keyphrase_match = re.search(r'\$\$[fF]ocus [kK]eyphrase\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
        slug_match = re.search(r'\$\$[sS]lug\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
        seo_metadesc_match = re.search(r'\$\$[mM]eta [dD]escription\$\$\s*[:\-]*\s*(.*?)\s*(?=\n*\$\$|$)', final_content, re.DOTALL)

        # Убираем пробелы в начале и конце строк
        data['title'] = title_match.group(1).strip() if title_match else None
        data['content'] = content_match.group(1).strip() if content_match else None
        data['tags'] = tags_match.group(1).strip() if tags_match else None
        data['seo_title'] = seo_title_match.group(1).strip() if seo_title_match else None
        data['seo_focuskw'] = focus_keyphrase_match.group(1).strip() if focus_keyphrase_match else None
        data['seo_slug'] = slug_match.group(1).strip() if slug_match else None
        data['seo_metadesc'] = seo_metadesc_match.group(1).strip() if seo_metadesc_match else None
        
       # Добавляем стандартные теги в начало списка, если они еще не добавлены
        if data['tags']:
            data['tags'] = "Tennis, Tennis news, " + data['tags']
        else:
            data['tags'] = "Tennis, Tennis news"  # Если теги пусты, добавляем только стандартные

        # Проверка на наличие основного контента и заголовка
        if not data['title'] or not data['content']:
            logging.error(f"Контент или заголовок не были извлечены. Пропуск текущей новости. {data}")
            return None
        
        logging.debug("Данные поста успешно извлечены.")
        return data

    except AttributeError as e:
        logging.error(f"Ошибка при извлечении данных поста: {e}")
        return None

# Модифицированная функция process_news_post (Функция для обработки и сохранения поста новости)
def process_news_post(news_data, news_id, conn):
    logging.debug(f"Начало обработки новости ID {news_id} через ИИ.")

    # Скрапим контент и изображения
    scraped_content, images = scrape_content_with_playwright(news_data['link'])
    if not scraped_content:
        logging.error(f"Не удалось скрапить контент для новости ID {news_id}. Пропуск.")
        update_news_status(conn, news_id, tested_value=2)
        return

    # Запрос к ИИ для генерации контента
    status, result = fetch_news_from_perplexity(news_data['link'], news_data['title'], news_data['pub_date'], scraped_content, images)

    if status == 'valid' and result:
        final_content = result['choices'][0]['message']['content']

        # Проверяем, что final_content — это строка
        if not isinstance(final_content, str):
            logging.error(f"Ожидалась строка, но получен объект типа {type(final_content)}: {final_content}")
            return

        # Извлечение данных из результата работы ИИ
        post_data = extract_post_data(final_content)
        if post_data:
            # Сохраняем финальный контент в таблицу posts и информацию об изображениях
            post_id = save_news_to_db(conn, post_data, news_data['link'], news_id, images)
            logging.info(f"Новость {news_data['title']} успешно сохранена в таблицу 'posts' с ID {post_id}.")
            update_news_status(conn, news_id, tested_value=1)  # Успешная обработка
        else:
            logging.error(f"Не удалось извлечь данные для новости ID {news_id}. Пропуск.")
            update_news_status(conn, news_id, tested_value=2)  # Не удалось обработать
    else:
        logging.error(f"Не удалось сгенерировать контент для новости ID {news_id}.")
        update_news_status(conn, news_id, tested_value=2)  # Не удалось обработать

# Функция для получения новостей из RSS-ленты по URL

# Словарь для хранения рефереров по доменам
referer_mapping = {
    "www.tennisworldusa.org": "https://www.tennisworldusa.org/",
    "tennisworldusa.org": "https://www.tennisworldusa.org/",
    "www.cbc.ca": "https://www.cbc.ca/",
    "www.tennisnow.com": "https://www.tennisnow.com/",
    "feeds.bbci.co.uk": "https://www.bbc.co.uk/",
    "austintennisacademy.com": "https://austintennisacademy.com/",
    "blog.scarboroughtennis.com.au": "https://blog.scarboroughtennis.com.au/",
    "blog.tennisplaza.com": "https://blog.tennisplaza.com/",
    "blog.tradesharktennis.com": "https://blog.tradesharktennis.com/",
    "chandosltc.com": "https://chandosltc.com/",
    "davidlinebarger.com": "https://davidlinebarger.com/",
    "feed.podbean.com": "https://podbean.com/",
    "racquetclub1.com": "https://racquetclub1.com/",
    "racquetsocial.com": "https://racquetsocial.com/",
    "tenngrand.com": "https://tenngrand.com/",
    "tennis-shot.com": "https://tennis-shot.com/",
    "www.10sballs.com": "https://www.10sballs.com/",
    "www.espn.com": "https://www.espn.com/",
    "www.theguardian.com": "https://www.theguardian.com/",
    "www.thehindu.com": "https://www.thehindu.com/",
    "www.ustaflorida.com": "https://www.ustaflorida.com/",
    # Добавьте другие домены и их рефереры по необходимости
}

# Список доменов, для которых требуется использование Playwright
playwright_required_domains = [
    "www.tennisworldusa.org",
    # Добавьте другие домены, если необходимо
]

# Инициализация Playwright браузера глобально
playwright_browser = None
browser_lock = Lock()

def initialize_playwright_browser():
    global playwright_browser
    with browser_lock:
        if not playwright_browser:
            playwright_browser = sync_playwright().start().chromium.launch(headless=True)
            logging.info("Playwright браузер инициализирован.")

def close_playwright_browser():
    global playwright_browser
    with browser_lock:
        if playwright_browser:
            playwright_browser.close()
            playwright_browser = None
            logging.info("Playwright браузер закрыт.")

def get_referer(rss_feed_url):
    """
    Получает реферер на основе домена RSS-ленты.
    Если домен не найден в referer_mapping, возвращает None.
    """
    parsed_url = urlparse(rss_feed_url)
    domain = parsed_url.netloc.lower()
    
    # Удаляем префикс 'www.' для более гибкого сопоставления
    if domain.startswith("www."):
        domain_no_www = domain[4:]
    else:
        domain_no_www = domain
    
    # Попробуем найти сначала с 'www.', затем без
    return referer_mapping.get(domain, referer_mapping.get(domain_no_www, None))

def fetch_rss_with_playwright(rss_feed_url):
    """
    Получает содержимое RSS-ленты с помощью Playwright.
    """
    try:
        initialize_playwright_browser()
        context = playwright_browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 Safari/537.36",
            locale="en-US",
        )
        page = context.new_page()
        
        # Получаем реферер для текущей ленты
        referer = get_referer(rss_feed_url)
        
        # Перейти на главную страницу, чтобы установить необходимые куки
        if referer:
            main_url = referer
        else:
            main_url = "https://www.tennisworldusa.org/"  # Установите дефолтный URL, если реферер не определён
        
        logging.info(f"Playwright: Переходим на {main_url} для установки куки.")
        response = page.goto(main_url, timeout=60000)
        if not response or response.status != 200:
            logging.error(f"Playwright: Не удалось загрузить главную страницу {main_url}. Статус: {response.status if response else 'No Response'}")
            page.close()
            context.close()
            return []
        
        # Теперь перейдем непосредственно на RSS-фид
        logging.info(f"Playwright: Переходим на RSS-фид {rss_feed_url}.")
        response = page.goto(rss_feed_url, timeout=60000)
        if not response or response.status != 200:
            logging.error(f"Playwright: Не удалось загрузить RSS-фид {rss_feed_url}. Статус: {response.status if response else 'No Response'}")
            page.close()
            context.close()
            return []
        
        content = page.content()
        page.close()
        context.close()
        
        feed = feedparser.parse(content)
        if feed.bozo:
            logging.error(f"Playwright: Ошибка в парсинге RSS-ленты: {rss_feed_url}. Ошибка: {feed.bozo_exception}")
            return []
        return feed.entries
    except Exception as e:
        logging.error(f"Playwright: Ошибка при получении RSS-ленты {rss_feed_url} с помощью Playwright: {e}")
        return []

# Модифицированная функция для получения новостей из RSS-ленты по URL
def get_news_from_rss_feed(rss_feed_url):
    logging.info(f"Получаем данные с RSS-ленты: {rss_feed_url}")
    
    parsed_url = urlparse(rss_feed_url)
    domain = parsed_url.netloc.lower()
    
    referer = get_referer(rss_feed_url)
    
    # Создаём scraper с настройками браузера, похожими на реальные
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False,
        }
    )
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/129.0.0.0 Safari/537.36",  # Обновите до актуального User-Agent
        "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    if referer:
        headers["Referer"] = referer
    
    scraper.headers.update(headers)
    
    max_attempts = 3
    attempt = 0
    while attempt < max_attempts:
        try:
            response = scraper.get(rss_feed_url, timeout=60, allow_redirects=True)
            response.raise_for_status()
            feed = feedparser.parse(response.content)
            if feed.bozo:
                logging.error(f"Ошибка в парсинге RSS-ленты: {rss_feed_url}. Ошибка: {feed.bozo_exception}")
                return []
            return feed.entries
        except requests.exceptions.HTTPError as http_err:
            if hasattr(response, 'status_code') and response.status_code == 403:
                logging.error(f"403 Forbidden при доступе к RSS-ленте: {rss_feed_url}.")
                return []
            else:
                logging.error(f"HTTP ошибка при получении данных с RSS-ленты {rss_feed_url}: {http_err}")
                return []
        except requests.Timeout:
            attempt += 1
            logging.error(f"Тайм-аут при попытке получения данных с RSS-ленты: {rss_feed_url}, попытка {attempt}")
            time.sleep(5)  # Ожидание перед повторной попыткой
        except requests.RequestException as e:
            logging.error(f"Ошибка при запросе к RSS-ленте {rss_feed_url}: {e}")
            return []
    return []

def process_rss_feed_stream(rss_feed_url, conn):
    logging.info(f"Обрабатываем RSS-ленту: {rss_feed_url}")
    news_items = get_news_from_rss_feed(rss_feed_url)

    for entry in news_items:
        news_data = process_rss_feed_entry(entry, rss_feed_url)

        # Проверяем наличие новости в базе данных
        if not check_news_in_db(conn, news_data['link']):
            # Сохраняем новость в БД
            news_id = save_news_in_db(conn, news_data)
            logging.info(f"Новость {news_data['title']} сохранена с ID {news_id}.")

            # Передаем данные на обработку модели ИИ
            process_news_post(news_data, news_id, conn)

def process_rss_feed_entry(entry, rss_feed_url):
    """
    Обрабатывает одну запись из RSS фида.
    """
    title = entry.get('title', 'Без названия').strip()
    link = entry.get('link', '')

    # Извлекаем содержимое статьи
    content = entry.get('content:encoded') or entry.get('description', '')
    pub_date = entry.get('pubDate') or entry.get('published', None)
    tags = entry.get('category', '')

    # Собираем все данные в единый словарь
    news_data = {
        'title': title,
        'link': link,
        'pub_date': pub_date,
        'content': content,
        'tags': tags,
        'source_url': rss_feed_url  # URL источника фида
    }
    return news_data

def clean_html(html_content):
    """
    Очищает HTML контент от скриптов и других ненужных элементов.
    """
    # Удаление всех <script> и <style> тегов
    soup = BeautifulSoup(html_content, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()

    # Возвращаем текстовое содержимое
    clean_text = soup.get_text(separator="\n").strip()
    
    # Удаление лишних пробелов и пустых строк
    clean_text = re.sub(r'\n\s*\n+', '\n', clean_text)
    return clean_text

# Функция для обработки записей фида
def process_feed_entry(entry, last_pub_date):
    pub_date = entry.get('published_parsed') or entry.get('updated_parsed')
    if pub_date:
        pub_date = datetime.fromtimestamp(time.mktime(pub_date))  # Преобразование time.struct_time в datetime

    if pub_date and pub_date <= last_pub_date:
        return  # Пропуск старых новостей

    title = entry.title
    source_url = entry.link
    content = entry.get('summary', '')

    # Сохранение новости в БД
    if not check_news_in_db(conn, source_url):
        status, result = fetch_news_from_perplexity(source_url, title, pub_date, content)
        if status == 'valid' and result:
            post_data = extract_post_data(result['content'])
            if post_data:
                save_news_in_db(conn, post_data['title'], source_url, post_data['content'])
        elif status == 'failed_to_download':
            logging.info(f"Попытка скраппинга для URL: {source_url}")
            content, images = scrape_content_with_playwright(source_url)
            if content:
                save_news_in_db(conn, title, source_url, content)

# Функция для обработки локально скачанных RSS-файлов
def process_local_rss_files(conn, directory="./rsstmp"):
    """
    Обрабатываем все локальные RSS файлы.
    Если новостей нет, это логируется и продолжаем работу.
    """
    processed_news = 0  # Счетчик обработанных новостей
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            logging.info(f"Обработка локального RSS файла: {file_path}")
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    feed = feedparser.parse(file.read())
                    last_pub_date = get_last_pub_date(conn)
                    for entry in feed.entries:
                        process_feed_entry(entry, last_pub_date)
                        processed_news += 1  # Увеличиваем счетчик новостей
            except Exception as e:
                logging.error(f"Ошибка при обработке файла {file_path}: {e}")
    
    if processed_news == 0:
        logging.info("Нет новых новостей в локальных фидах.")
    else:
        logging.info(f"Обработано {processed_news} новостей из локальных фидов.")


# Модифицированная функция fetch_news_from_perplexity
def fetch_news_from_perplexity(news_url, title, pub_date, scraped_content, images):
    try:
        # Генерируем список изображений для передачи в модель
        images_info = [{"url": img[0], "alt": img[1]} for img in images]

        # Формируем сообщения для модели, включая информацию об изображениях
        messages = [
            {
                "role": "system",
                "content": (
                    f"You are a journalist specializing in tennis. You are provided with the full content of the news article below."
                    f"Before generating the article, follow these steps:\n"
                    f"1. Ensure the content is related to tennis. If it is not related to tennis, return only the phrase: $$not_tennis_news$$.\n"
                    f"2. Analyze the current top trends in search queries related to tennis, and integrate relevant information into the article and SEO data.\n"
                    f"3. Using an Internet search, familiarize yourself with the most relevant data for this news to better immerse yourself in the content of the events described in the article.\n"
                    f"4. Use the provided content to generate the article and ensure that images from the original content are included in the text, formatted as: ![Image Description](Image URL).\n\n"
                    f"Full content: {scraped_content}\n\n"
                    f"Images: {images_info}\n\n"
                    f"Ensure the response **strictly adheres to the following format** and contains no additional comments or text beyond the requested information.\n\n"
                    f"Proceed to generate the article according to the following sections:(without formatting with '*')\n"
                    f"1. $$title$$: <Provide title here>\n"
                    f"2. $$Content$$: <Provide the content of the article here>\n"
                    f"3. $$Tags$$: <Provide tags here, separated by commas>\n"
                    f"4. $$SEO Title$$: <Provide an SEO optimized title here>\n"
                    f"5. $$Focus Keyphrase$$: <Provide a focus keyphrase here>\n"
                    f"6. $$Slug$$: <Provide SEO-friendly slug here>\n"
                    f"7. $$Meta Description$$: <Provide a meta description here>\n"
                    f"Make sure the structure is followed exactly as described above."
                )
            },
            {
                "role": "user",
                "content": (
                    "Please write a single, comprehensive article based on the above information."
                    "Follow the structure strictly and include any opening or closing phrases, if appropriate, only in the $$Content$$ section."
                    "The article should be written on behalf of a young tennis player, Mia Johnson-Carter, living in Miami."
                    "Ensure that the article is well-structured, with appropriate headings and sections. "
                    "Additionally, analyze trending search topics and incorporate them where relevant. "
                    "Also, make sure to include images from the original article, formatted correctly."
                    "When writing in a conversational or informal tone, subtly incorporate elements of the Miami dialect. These should not be overly noticeable or distracting, but should include phrases or expressions that would be familiar to native Miami residents."
                    f"At the end of the article, include the following phrase: 'If you want, you can check out the news where I found it [here]({news_url})' and ensure the URL is hyperlinked correctly."
                    "Sign off the article with a friendly phrase such as 'Yours truly, Mia' or something similar that resonates with the Miami vibe.\n\n"
                    "The article should strictly include the required sections in the correct format without any additional comments or phrases."
                    "Use the latest search trends and keywords to optimize the content for better search engine visibility."
                    "The article you create (content section) will be published in a blog created on WordPress. Make sure that the images present in the original news will be displayed in the article you create."
                )
            }
        ]

        # Формируем данные для запроса
        payload = {
            "model": "llama-3.1-sonar-small-128k-online",
            "messages": messages,
            "max_tokens": 2500,
            "temperature": 0.7,
            "top_p": 0.9,
            "return_citations": True,
            "search_domain_filter": ["perplexity.ai"],
            "return_images": True,
            "return_related_questions": False,
            "search_recency_filter": "month",
            "top_k": 0,
            "stream": False,
            "presence_penalty": 0,
            "frequency_penalty": 1
        }

        headers = {
            'Authorization': f'Bearer {API_KEY}',
            'Content-Type': 'application/json'
        }
        logging.debug(f"Отправляемые данные в Perplexity: {payload}")
        
        # Выполняем запрос к Perplexity API
        response = requests.post(PERPLEXITY_API_URL, headers=headers, json=payload)

        # Проверка успешного выполнения запроса
        if response.status_code == 200:
            result = response.json()
            # logging.debug(f"Полученный ответ от Perplexity: {result}")
            # Проверяем специальные фразы в ответе
            if '$$not_tennis_news$$' in result:
                return 'not_tennis_news', None
            if '$$Failed_to_download$$' in result:
                return 'failed_to_download', None

            return 'valid', result
        else:
            logging.error(f"Ошибка при запросе к Perplexity API: {response.status_code} - {response.text}")
            return 'failed_to_download', None

    except Exception as e:
        logging.error(f"Ошибка при работе с Perplexity API: {e}")
        return 'failed_to_download', None

# Загрузка RSS-файлов
# Модифицированная функция для скачивания RSS-фидов с динамическим Referer и использованием Playwright
def download_rss_feeds(rss_urls, download_dir="./rsstmp"):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    for url in rss_urls:
        referer = get_referer(url)
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()

        # Создаём scraper с настройками браузера, похожими на реальные
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False,
            }
        )
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/129.0.0.0 Safari/537.36",  # Обновите до актуального User-Agent
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        if referer:
            headers["Referer"] = referer
        scraper.headers.update(headers)
        
        attempt = 0
        success = False
        while attempt < 3 and not success:
            try:
                response = scraper.get(url, timeout=120)  # Тайм-аут увеличен до 120 сек.
                response.raise_for_status()
                filename = os.path.join(download_dir, url.split("/")[-1] + ".rss")
                with open(filename, 'wb') as file:
                    file.write(response.content)
                logging.info(f"RSS фид скачан и сохранен: {filename}")
                success = True
            except requests.exceptions.RequestException as e:
                attempt += 1
                wait_time = 5 * attempt  # Время ожидания увеличивается на каждой попытке
                logging.error(f"Ошибка при скачивании RSS фида: {e}. Повторная попытка через {wait_time} секунд.")
                time.sleep(wait_time)
        
        if not success:
            logging.error(f"Не удалось скачать RSS фид после {attempt} попыток: {url}")

# Очистка временной папки со скачанными файлами RSS
def clean_rss_directory(directory="./rsstmp"):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Удален файл: {file_path}")
        except Exception as e:
            logging.error(f"Ошибка при удалении файла {file_path}: {e}")


# В основной функции main(), никаких изменений не требуется, если все остальные функции работают корректно
def main():
    conn = get_db_connection()

    if conn:
        # Онлайн обработка RSS
        online_rss_urls = [
            # Ваши RSS URL
        "https://www.cbc.ca/webfeed/rss/rss-sports-tennis",
        # "https://www.tennisnow.com/cmspages/blogrss.aspx",
        "https://feeds.bbci.co.uk/sport/tennis/rss.xml",
        "https://www.tennisworldusa.org/rss/news.php",
        # "https://cincinnatiopen.com/news/feed/",
        # "http://www.tennisviewmag.com/rss.xml",
        "http://www.worldtennismagazine.com/feed",
        # "https://austintennisacademy.com/feed/",
        # "https://blog.scarboroughtennis.com.au/feed/",
        # "https://blog.tennisplaza.com/feed/",
        # "https://blog.tradesharktennis.com/feed/",
        # "https://chandosltc.com/feed/",
        # "https://davidlinebarger.com/feed/",
        # "https://feed.podbean.com/essentialtennis/feed.xml",
        # "https://feeds.bbci.co.uk/sport/tennis/rss.xml",
        # "https://feeds.feedburner.com/blogspot/sIQOA",
        # "https://feeds.feedburner.com/DaughtersTennisPassion",
        # "https://feeds.feedburner.com/Grandslamgal",
        # "https://feeds.feedburner.com/tennis-australia",
        # "https://feeds.feedburner.com/TennisViewer",
        # "https://feeds.feedburner.com/tennisx",
        # "https://feeds.feedburner.com/WomensTennisBlog",
        # "https://fiendatcourt.com/feed/",
        # "https://ftw.usatoday.com/category/tennis/feed",
        # "https://guidemytennis.com/tennis-articles/",
        # "https://highgate-tennis.co.uk/feed/",
        # "https://littleyellowball.blog/feed/",
        # "https://mcshowblog.com/feed/",
        # "https://montrealgazette.com/category/sports/tennis/feed.xml",
        # "https://mootennis.com/feed/",
        # "https://novakdjokovic.com/en/feed/",
        # "https://pelotista.com/feed/",
        # "https://racquetclub1.com/feed/",
        # "https://racquetsocial.com/feed/",
        # "https://reviewsfortennis.com/feed/",
        # "https://rogerfederer.com/index.php/news?format=feed&type=rss",
        # "https://seniortennisblog.com/feed/",
        # "https://tenngrand.com/category/news/feed/",
        # "https://tenngrand.com/feed/",
        # "https://tennis-shot.com/feed/",
        # "https://tennisabides.com/feed/",
        # "https://tenniscamper.com/feed/",
        # "https://tennisconnected.com/home/feed/",
        # "https://tenniseventguide.com/feed",
        # "https://tennispal.com/feed/",
        # "https://tennisproguru.com/category/blog/feed/",
        # "https://the-tennisnews.blogspot.com/feeds/posts/default?alt=rss",
        # "https://thetennisfoodie.com/blog/feed/",
        # "https://thetenniswhisperer.blogspot.com/feeds/posts/default",
        # "https://toomanyrackets.com/feed/",
        # "https://totallytennis.wordpress.com/feed/",
        # "https://tribeathletics.com/rss?path=mten",
        # "https://wearecollegetennis.com/feed/",
        "https://www.10sballs.com/feed/",
        # "https://www.asiantennis.com/feed/",
        # "https://www.atptour.com/en/media/rss-feed/xml-feed",
        # "https://www.braingametennis.com/feed/",
        # "https://www.brisbaneinternational.com.au/feed/",
        # "https://www.chatswoodtennis.com.au/feed/",
        # "https://www.cliffordandoak.com/blogs/coaching.atom",
        # "https://www.espn.com/espn/rss/tennis/news",
        # "https://www.mattspoint.com/blog?format=RSS",
        # "https://www.maximizingtennispotential.com/feed/",
        # "https://www.perfect-tennis.com/feed/",
        # "https://www.resourcelymarketing.com/feed/",
        # "https://www.sbnation.com/rss/current",
        # "https://www.sportsnet.ca/tennis/feed",
        # "https://www.standard.co.uk/sport/tennis/rss",
        # "https://www.tennis365.com/feed",
        # "https://www.tennisabstract.com/blog/feed/",
        # "https://www.tennisconsult.com/feed/",
        # "https://www.tenniseurope.org/newsfeed/0",
        # "https://www.tennisfitness.com/blog.rss",
        # "https://www.tennisireland.ie/feed",
        # "https://www.theeyecoach.com/blogs/news.atom",
        # "https://www.theguardian.com/sport/tennis/rss",
        # "https://www.thehindu.com/sport/tennis/?service=rss",
        # "https://www.theslicetennis.com/articles?format=rss",
        # "https://www.ustaflorida.com/latest-news/feed",
        # "https://www.yardbarker.com/rss/sport/10"
        ]
        for rss_feed_url in online_rss_urls:
            process_rss_feed_stream(rss_feed_url, conn)

        # Оффлайн обработка RSS
        offline_rss_urls = [
            "https://www.usopen.org/en_US/news/rss/usopen.rss"
        ]
        download_rss_feeds(offline_rss_urls)
        process_local_rss_files(conn)
        clean_rss_directory()

        # Теперь запускаем процесс обработки новостей и отправки их модели
        process_unprocessed_news(conn)

        # Логируем успешное завершение всех процессов
        logging.info("Все процессы завершены. Обработка всех новостей завершена.")

        # Закрытие соединения с базой данных
        conn.close()

        # Закрытие Playwright браузера
        close_playwright_browser()

if __name__ == "__main__":
    main()
