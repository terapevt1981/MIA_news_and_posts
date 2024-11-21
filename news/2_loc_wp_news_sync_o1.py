import psycopg2
import requests
import logging
from datetime import datetime
import os
import json
import urllib3

# Подавление предупреждений SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройка логирования
log_directory = "/home/ubuntu/scripts/mia/news/log/"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logging.basicConfig(
    filename=os.path.join(log_directory, f"2-wp_sync_{datetime.now().strftime('%Y-%m-%d')}.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Конфигурация подключения к базе данных PostgreSQL
db_config = {
    'dbname': 'miatennispro',
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': '5432'
}

# Конфигурация подключения к WordPress API
wp_config = {
    'api_url': 'https://miatennispro.com/wp-json/wp/v2/posts',
    'api_url_v2': 'https://miatennispro.com/wp-json/wp/v2',
    'media_url': 'https://miatennispro.com/wp-json/wp/v2/media',
    'token_url': 'https://miatennispro.com/wp-json/jwt-auth/v1/token',
    'username': os.getenv('WP_USERNAME'),
    'password': os.getenv('WP_PASSWORD'),
    'auth_token': None
}

# Добавляем JSON-LD к содержимому постов перед отправкой в WordPress
def add_structured_data_to_content(title, content, publish_date):
    structured_data = {
        "@context": "https://schema.org",
        "@type": "NewsArticle",
        "headline": title,
        "datePublished": publish_date.isoformat(),
        "articleBody": content
    }
    
    # Конвертируем словарь в строку JSON
    json_ld = f'<script type="application/ld+json">{json.dumps(structured_data)}</script>'
    
    # Добавляем JSON-LD в конец контента статьи
    return content + json_ld

# Создаем сессию для запросов и устанавливаем заголовок Host
session = requests.Session()
session.headers.update({'Host': 'miatennispro.com'})

def get_db_connection():
    try:
        conn = psycopg2.connect(**db_config)
        logging.info("Подключение к базе данных успешно выполнено.")
        return conn
    except Exception as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        return None

def fetch_pre_draft_posts(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT id, title, content, tags, pub_date AS publish_date, category_id, seo_title, seo_metadesc, seo_focuskw, seo_slug
            FROM posts
            WHERE status = 'pre-Draft' AND category_id = 8
        """)
        posts = cursor.fetchall()
        logging.info(f"Найдено {len(posts)} постов со статусом 'pre-Draft' для отправки в WordPress.")
        return posts

def get_new_token():
    try:
        response = session.post(
            wp_config['token_url'],
            json={"username": wp_config['username'], "password": wp_config['password']},
            verify=False
        )
        if response.status_code == 200:
            token = response.json().get("token")
            logging.info("Успешное получение нового токена.")
            return token
        else:
            logging.error(f"Ошибка при получении нового токена: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса на получение токена: {e}")
        return None

def get_or_create_tag(tag_name):
    try:
        response = session.get(
            f"{wp_config['api_url_v2']}/tags",
            params={"search": tag_name},
            verify=False
        )
        if response.status_code == 200:
            tags = response.json()
            if tags:
                return tags[0]['id']
            else:
                response = session.post(
                    f"{wp_config['api_url_v2']}/tags",
                    json={"name": tag_name},
                    verify=False
                )
                if response.status_code == 201:
                    return response.json()['id']
                else:
                    logging.error(f"Ошибка при создании тега {tag_name}: {response.status_code} - {response.text}")
                    return None
        else:
            logging.error(f"Ошибка при проверке тега {tag_name}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Ошибка при обработке тега {tag_name}: {e}")
        return None

def upload_image_to_wordpress(image_url):
    try:
        response = requests.get(image_url)
        if response.status_code == 200:
            image_data = response.content
            filename = os.path.basename(image_url)
            mime_type = response.headers.get('Content-Type', 'image/jpeg')

            media_headers = {
                "Authorization": f"Bearer {wp_config['auth_token']}",
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": mime_type
            }

            media_response = session.post(
                wp_config['media_url'],
                headers=media_headers,
                data=image_data,
                verify=False
            )

            if media_response.status_code in [200, 201]:
                media_json = media_response.json()
                attachment_id = media_json.get('id')
                media_url = media_json.get('source_url')
                logging.info(f"Изображение загружено в WordPress: {media_url}")
                return attachment_id, media_url
            else:
                logging.error(f"Ошибка при загрузке изображения в WordPress: {media_response.status_code} - {media_response.text}")
                return None, None
        else:
            logging.error(f"Не удалось загрузить изображение по URL {image_url}: {response.status_code}")
            return None, None
    except Exception as e:
        logging.error(f"Ошибка при загрузке изображения: {e}")
        return None, None

def process_images_in_content(content, conn, post_id):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')

    cursor = conn.cursor()
    cursor.execute("SELECT image_url, alt_text FROM post_images WHERE post_id = %s", (post_id,))
    images = cursor.fetchall()

    for img in soup.find_all('img'):
        img_src = img.get('src')
        for image in images:
            if img_src == image[0]:
                attachment_id, wp_image_url = upload_image_to_wordpress(img_src)
                if attachment_id and wp_image_url:
                    img['src'] = wp_image_url
                    cursor.execute("""
                        UPDATE post_images
                        SET wp_attachment_id = %s, wp_image_url = %s
                        WHERE post_id = %s AND image_url = %s
                    """, (attachment_id, wp_image_url, post_id, img_src))
                    conn.commit()
                else:
                    logging.error(f"Не удалось обработать изображение {img_src}")
    updated_content = str(soup)
    return updated_content

def send_posts_to_wordpress(conn):
    posts = fetch_pre_draft_posts(conn)
    if not posts:
        logging.info("Нет постов со статусом 'pre-Draft' для отправки в WordPress.")
        return

    if wp_config['auth_token'] is None:
        wp_config['auth_token'] = get_new_token()
        if wp_config['auth_token'] is None:
            logging.error("Не удалось получить токен для доступа к WordPress API.")
            return

    session.headers.update({"Authorization": f"Bearer {wp_config['auth_token']}"})
    cursor = conn.cursor()

    for post in posts:
        (post_id, title, content, tags, publish_date, category_id, seo_title, seo_metadesc, seo_focuskw, seo_slug) = post
        
        # Добавляем структурированные данные к контенту
        content = add_structured_data_to_content(title, content, publish_date)
        
        # Обработка изображений и т.д.
        content = process_images_in_content(content, conn, post_id)
        
        # Далее идёт отправка поста в WordPress


        tag_ids = []
        for tag_name in tags.split(','):
            tag_name = tag_name.strip()
            if tag_name:
                tag_id = get_or_create_tag(tag_name)
                if tag_id:
                    tag_ids.append(tag_id)

        cursor.execute("SELECT image_url FROM post_images WHERE post_id = %s LIMIT 1", (post_id,))
        featured_image = cursor.fetchone()
        if featured_image:
            featured_image_id, wp_featured_image_url = upload_image_to_wordpress(featured_image[0])
        else:
            featured_image_id = None

        post_data = {
            "title": title,
            "content": content,
            "status": "publish",
            "date": publish_date.isoformat(),
            "categories": [category_id],
            "tags": tag_ids,
        }

        if featured_image_id:
            post_data['featured_media'] = featured_image_id

        response = session.post(
            wp_config['api_url'],
            json=post_data,
            verify=False
        )

        if response.status_code == 201:
            wp_post_id = response.json()["id"]
            logging.info(f"Пост успешно отправлен в WordPress с ID {wp_post_id}. Обновление базы данных...")

            cursor.execute(
                "UPDATE posts SET status = %s, wp_post_id = %s WHERE id = %s",
                ("publish", wp_post_id, post_id)
            )
            conn.commit()

            update_meta_data(wp_post_id, seo_title, seo_metadesc, seo_focuskw, seo_slug)

        elif response.status_code == 403 and "jwt_auth_invalid_token" in response.text:
            logging.info("Токен истек, получаем новый токен...")
            new_token = get_new_token()
            if new_token:
                wp_config['auth_token'] = new_token
                session.headers.update({"Authorization": f"Bearer {wp_config['auth_token']}"})
                response = session.post(
                    wp_config['api_url'],
                    json=post_data
                )
                if response.status_code == 201:
                    wp_post_id = response.json()["id"]
                    logging.info(f"Пост успешно отправлен в WordPress с ID {wp_post_id}. Обновление базы данных...")

                    cursor.execute(
                        "UPDATE posts SET status = %s, wp_post_id = %s WHERE id = %s",
                        ("publish", wp_post_id, post_id)
                    )
                    conn.commit()

                    update_meta_data(wp_post_id, seo_title, seo_metadesc, seo_focuskw, seo_slug)
                else:
                    logging.error(f"Ошибка при повторной отправке поста в WordPress: {response.status_code} - {response.text}")
            else:
                logging.error("Не удалось получить новый токен для повторной отправки поста в WordPress.")
        else:
            logging.error(f"Ошибка при отправке поста в WordPress: {response.status_code} - {response.text}")

    cursor.close()

def update_meta_data(wp_post_id, seo_title, seo_metadesc, seo_focuskw, seo_slug):
    meta_fields = {
        "_yoast_wpseo_title": seo_title,
        "_yoast_wpseo_metadesc": seo_metadesc,
        "_yoast_wpseo_focuskw": seo_focuskw,
        "_yoast_wpseo_slug": seo_slug,
        "_yoast_wpseo_article_type": "news article"
    }

    for key, value in meta_fields.items():
        try:
            meta_data = {
                "meta": {
                    key: value
                }
            }
            response = session.put(
                f"{wp_config['api_url']}/{wp_post_id}",
                json=meta_data
            )

            if response.status_code == 200:
                logging.info(f"Мета-данное {key} успешно обновлено для поста ID {wp_post_id}.")
            else:
                logging.error(f"Ошибка при обновлении мета-данного {key}: {response.status_code} - {response.text}")

        except Exception as e:
            logging.error(f"Ошибка при обновлении мета-данного {key} для поста ID {wp_post_id}: {e}")

def main():
    conn = get_db_connection()
    if conn:
        send_posts_to_wordpress(conn)
        conn.close()

if __name__ == "__main__":
    main()
