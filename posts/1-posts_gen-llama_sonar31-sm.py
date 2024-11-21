import requests
import psycopg2
from datetime import datetime, timedelta
import re
import logging
import urllib.parse
import os
import json

# Configure logging
def configure_logging():
    """
    Настраивает логирование для скрипта.
    Создает директорию для логов, если она не существует, и настраивает формат логов.
    """
    log_directory = "/home/ubuntu/scripts/mia/posts/log/"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    log_file = os.path.join(log_directory, f"1-posts_gen_{datetime.now().strftime('%Y-%m-%d')}.log")
    
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,  # Установите на logging.INFO для уменьшения подробности
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

# Database configuration
db_config = {
    'dbname': 'miatennispro',
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': '5432'
}

# Perplexity AI API configuration
perplexity_api_key = os.getenv('API_KEY_PERPLEXITY')

# Pixabay API configuration
pixabay_api_key = os.getenv('API_KEY_PIXABAY')

# Function to establish a connection to the PostgreSQL database
def get_db_connection():
    try:
        conn = psycopg2.connect(**db_config)
        logging.info("Connected to the database successfully.")
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        return None

# Function to retrieve specified categories
def get_categories():
    categories = [
        {'category_id': 20, 'category_name': 'My Personal Blog'},
        {'category_id': 23, 'category_name': 'Nutrition and Health Blog'},
        {'category_id': 21, 'category_name': 'Tennis Fashion Blog'},
        {'category_id': 22, 'category_name': 'Tennis PRO Blog'},
        {'category_id': 219, 'category_name': 'Tennis Tips'}
    ]
    logging.debug(f"Using specified categories: {categories}")
    return categories

# Function to get keywords for a category using Pyusuggest (Google Autocomplete)
def get_keywords_for_category(category_name):
    url = 'http://suggestqueries.google.com/complete/search'
    params = {
        'client': 'firefox',
        'q': category_name,
        'hl': 'en'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            suggestions = response.json()[1]
            keywords = suggestions
            logging.debug(f"Keywords for category '{category_name}': {keywords}")
            return keywords
        else:
            logging.error(f"Failed to get keywords for category '{category_name}'. Status code: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Exception while fetching keywords for category '{category_name}': {e}")
        return []

# Function to retrieve existing themes for a category from the database
def get_existing_themes(conn, category_id):
    cursor = conn.cursor()
    try:
        query = "SELECT theme FROM blog_post_theme WHERE category_id = %s"
        cursor.execute(query, (category_id,))
        existing_themes = [row[0] for row in cursor.fetchall()]
        logging.debug(f"Existing themes for category {category_id}: {existing_themes}")
        return existing_themes
    except Exception as e:
        logging.error(f"Error fetching existing themes for category {category_id}: {e}")
        return []

# Function to create a prompt for generating new themes
def create_theme_prompt(category_name, existing_themes, keywords):
    existing_themes_str = '; '.join(existing_themes)
    keywords_str = ', '.join(keywords)
    system_prompt = {
        "role": "system",
        "content": (
            f"You are an SEO expert specializing in generating high-traffic blog topics. "
            f"Generate 5 unique blog post ideas for the category '{category_name}'. "
            f"Use the following keywords: {keywords_str}. "
            f"Avoid duplicating the following existing topics: {existing_themes_str}. "
            "Each idea should include a title and a brief description, optimized for SEO and potential for affiliate marketing."
        )
    }
    user_prompt = {
        "role": "user",
        "content": (
            "Please provide the blog post ideas as a numbered list in the following format:\n"
            "1. Title: Description\n"
            "2. Title: Description\n"
            "...\n"
            "Do not use any markdown formatting or additional characters."
            f"Be sure to create topics for category '{category_name}'."
        )
    }
            # f"Be sure to create topics for category 'Tennis Tips' about the correct choice of balls, rackets, about the sizes and varieties of rackets, about re-stringing the racket grip (what they are)"

    logging.debug(f"Created theme prompts for category '{category_name}'.")
    return [system_prompt, user_prompt]

# Function to generate new themes using the Perplexity.ai API
def generate_new_themes(prompts, perplexity_api_key):
    payload = {
        "model": "llama-3.1-sonar-small-128k-online",
        "messages": prompts,
        "max_tokens": 500,
        "temperature": 0.7,
        "top_p": 0.9,
        "stream": False
    }
    headers = {
        'Authorization': f'Bearer {perplexity_api_key}',
        'Content-Type': 'application/json'
    }
    logging.debug(f"Sending request to Perplexity.ai API with payload: {payload}")
    try:
        response = requests.post('https://api.perplexity.ai/chat/completions', headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            logging.debug(f"Received response from Perplexity.ai API: {data}")
            return data['choices'][0]['message']['content']
        else:
            logging.error(f"Error in API request: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Exception during API request: {e}")
        return None

# Function to parse generated ideas from the model's response
def parse_generated_ideas(generated_content):
    """
    Парсит сгенерированный контент и извлекает идеи с заголовками и описаниями.
    
    Args:
        generated_content (str): Текст, сгенерированный моделью, содержащий идеи.
    
    Returns:
        list: Список словарей с ключами 'title' и 'description'.
    """
    ideas = []
    try:
        lines = generated_content.strip().split('\n')
        current_title = None

        for line in lines:
            stripped_line = line.strip()
            if not stripped_line:
                continue  # Пропускаем пустые строки

            # Проверяем, является ли строка заголовком
            # Примеры:
            # 1. **Title** - Description
            # 1. **Title**: Description
            # 1. **Title**
            # - **Title** - Description
            # - **Title**: Description
            # - **Title**
            title_match = re.match(r'^(\d+\.|-)\s*\*\*(.*?)\*\*\s*(?:[-:]\s*(.*))?$', stripped_line)
            if title_match:
                number_or_bullet = title_match.group(1)
                title = title_match.group(2).strip()
                description = title_match.group(3).strip() if title_match.group(3) else ''

                if description:
                    # Если описание уже есть на той же строке
                    ideas.append({'title': title, 'description': description})
                    current_title = None
                else:
                    # Описание на следующей строке(s)
                    current_title = title
                continue

            # Если текущий заголовок установлен, и строка начинается с дефиса или звездочки, то это описание
            if current_title:
                desc_match = re.match(r'^[-*]\s*(.+)', stripped_line)
                if desc_match:
                    description = desc_match.group(1).strip()
                    ideas.append({'title': current_title, 'description': description})
                    current_title = None
                else:
                    logging.warning(f"Expected description after title '{current_title}', but got: '{line}'")
                continue

            # Если строка не соответствует ни заголовку, ни описанию, логируем предупреждение
            logging.warning(f"Unrecognized line format: '{line}'")

        logging.debug(f"Extracted ideas: {ideas}")
        return ideas

    except Exception as e:
        logging.error(f"Error parsing generated ideas: {e}")
        return []

# Function to save a new theme to the database
def save_theme_to_db(conn, category_id, theme_title, theme_description, keywords):
    cursor = conn.cursor()
    try:
        # Check if the theme already exists
        cursor.execute("SELECT 1 FROM blog_post_theme WHERE theme = %s AND category_id = %s", (theme_title, category_id))
        if cursor.fetchone():
            logging.warning(f"Theme '{theme_title}' already exists in category {category_id}. Skipping.")
            return False
        else:
            query = """
            INSERT INTO blog_post_theme (category_id, theme, keywords, description)
            VALUES (%s, %s, %s, %s)
            """
            keywords_str = ', '.join(keywords)
            cursor.execute(query, (category_id, theme_title, keywords_str, theme_description))
            conn.commit()
            logging.info(f"Saved new theme '{theme_title}' to database.")
            return True
    except Exception as e:
        logging.error(f"Error saving theme '{theme_title}': {e}")
        conn.rollback()
        return False

# Function to create a prompt for generating an article based on a theme
def create_article_prompt(theme, description, keywords, theme_id, conn):
    keywords_str = ', '.join(keywords)
    
    # Additional context data
    blog_url = "https://miatennispro.com"  # Replace with your actual blog URL
    pub_date = datetime.now().strftime("%Y-%m-%d")
    title = theme  # Article title

    system_prompt = {
        "role": "system",
        "content": (
            "You are a professional content writer specializing in SEO-optimized articles on behalf of Mia Johnson-Carter, a 24-year-old aspiring professional tennis player living in Miami. "
            "You have access to the internet to gather information. "
            f"Your task is to write a comprehensive article on the topic '{theme}', using the provided keywords and description. "
            f"Naturally incorporate the following keywords: {keywords_str}. "
            "Visit the blog site and search for relevant articles in the same category as the given topic. "
            "Read and analyze several relevant articles to understand the style and tone of the blog. "
            "Write the article in your own style, ensuring it is engaging and informative. Include cross-links to relevant articles on the blog, formatted as: [Link Text](URL).\n"
            "Optimize the article for search engines, and ensure it is engaging and informative.\n"
            "Include meta title, meta description, focus keyphrase, and slug for SEO. "
            "Where appropriate, suggest products or services, and include placeholders for affiliate links using the format {{AFFILIATE_LINK:Product Name}}. Do not insert actual links. "
            "Include relevant images where appropriate using the placeholder {{IMAGE}}. Do not provide actual image URLs. "
            "Ensure the response **strictly adheres to the following format** and contains no additional comments or text beyond the requested information.\n\n"
            "Proceed to generate the article according to the following sections:\n"
            "- Title: <Provide title here>\n"
            "- Content: <Provide the content of the article here>\n"
            "- Tags: <Provide tags here, separated by commas>\n"
            "- SEO Title: <Provide an SEO optimized title here>\n"
            "- Focus Keyphrase: <Provide a focus keyphrase here>\n"
            "- Slug: <Provide SEO-friendly slug here>\n"
            "- Meta Description: <Provide a meta description here>\n\n"
            "**I provide data for content creation, including:**\n"
            f"Title: {title}\n"
            f"Description: {description}\n"
            f"Publication Date: {pub_date}\n"
            f"Keywords: {keywords_str}\n"
            f"Blog URL: {blog_url}"
        )
    }

    user_prompt = {
        "role": "user",
        "content": (
            "Please write a comprehensive article based on the provided topic and keywords. "
            "Ensure that the article is well-structured, with appropriate headings and sections. "
            "Visit the blog site and search for relevant articles in the same category to understand the style and tone. "
            "Include cross-links to relevant articles on the blog, formatted correctly. "
            "When writing in a conversational or informal tone, subtly incorporate elements of the blog's style. "
            f"At the end of the article, include the following phrase: 'If you want, you can check out more articles on our blog [here]({blog_url})' and ensure the URL is hyperlinked correctly. "
            "Sign off the article with a friendly phrase such as 'Yours truly' or something similar that resonates with the blog's style.\n"
            "Before submitting your response, ensure that all required components are present and in the correct format. I will list the expected components of the response:\n"
            f"- Title: <Provide title here> without formatting with '*'\n"
            f"- Content: <Provide the content of the post here>\n"
            "SEO-datas:(without formatting with '*')\n"
            f"Compose the requested data for SEO without additional formatting markers such as '*'"
            f"- Tags: <Provide tags here, separated by commas>\n"
            f"- SEO Title: <Provide an SEO optimized title here>\n"
            f"- Focus Keyphrase: <Provide a focus keyphrase here>\n"
            f"- Slug: <Provide SEO-friendly slug here>\n"
            f"- Meta Description: <Provide a meta description here>"
        )
    }
    logging.debug(f"Created article prompts for theme '{theme}' with stylistic requirements.")
    return [system_prompt, user_prompt]

# Function to create a prompt for the personal blog category
def create_personal_blog_prompt(theme, description, keywords):
    keywords_str = ', '.join(keywords)
    
    mia_biography = (
        "Mia Johnson-Carter is a 24-year-old aspiring professional tennis player living in Miami. "
        "She started playing tennis at the age of 5 and has been passionate about the sport ever since. "
        "Mia is involved in charity work and conservation of natural resources. "
        "Mia trains daily at the Miami Tennis Academy and dreams of competing in the Grand Slam tournaments. "
        "She balances her rigorous training schedule with college studies in sports psychology. "
        "Mia loves exploring Miami's vibrant culture, enjoys the beach, and spending time with her close friends and family."
    )
    
    blog_url = "https://miatennispro.com"  # Replace with your actual blog URL
    pub_date = datetime.now().strftime("%Y-%m-%d")
    title = theme  # Article title

    system_prompt = {
        "role": "system",
        "content": (
            f"You are writing a personal blog post as Mia Johnson-Carter. {mia_biography} "
            "You have access to the internet to gather information. "
            f"Your task is to write a comprehensive article on the topic '{theme}', using the provided keywords and description, incorporating personal experiences and emotions. "
            "Visit the blog site and search for relevant articles in the same category as the given topic. "
            "Read and analyze several relevant articles to understand the style and tone of the blog. "
            "Write the post in a warm, friendly, and relatable tone, often using local Miami expressions subtly. "
            f"Naturally include the following keywords: {keywords_str}. "
            "End the post with a friendly sign-off such as 'With love, Mia'. "
            "Include meta title, meta description, focus keyphrase, and slug for SEO. "
            "Include relevant images where appropriate using the placeholder {{IMAGE}}. Do not provide actual image URLs. "
            "Ensure the response **strictly adheres to the following format** and contains no additional comments or text beyond the requested information.\n\n"
            "Proceed to generate the blog post according to the following sections:\n"
            "- Title: <Provide title here> without formatting with '*'\n"
            "- Content: <Provide the content of the post here>\n"
            "SEO-datas:(without formatting with '*')\n"
            "- Tags: <Provide tags here, separated by commas>\n"
            "- SEO Title: <Provide an SEO optimized title here>\n"
            "- Focus Keyphrase: <Provide a focus keyphrase here>\n"
            "- Slug: <Provide SEO-friendly slug here>\n"
            "- Meta Description: <Provide a meta description here>\n\n"
            "**I provide data for content creation, including:**\n"
            f"Title: {title}\n"
            f"Description: {description}\n"
            f"Publication Date: {pub_date}\n"
            f"Keywords: {keywords_str}\n"
            f"Blog URL: {blog_url}"
        )
    }

    user_prompt = {
        "role": "user",
        "content": (
            "Please write a comprehensive blog post based on the provided topic and keywords. "
            "Ensure that the post is well-structured and reflects Mia's personal thoughts and feelings. "
            "Visit the blog site and search for relevant articles in the same category to understand the style and tone. "
            "Include cross-links to relevant articles on the blog, formatted correctly. "
            "When writing in a conversational or informal tone, subtly incorporate elements of the blog's style. "
            f"At the end of the post, include the following phrase: 'If you want, you can check out more articles on our blog [here]({blog_url})' and ensure the URL is hyperlinked correctly. "
            "Sign off the post with a friendly phrase such as 'With love, Mia' or something similar that resonates with the blog's style.\n"
            "Before submitting your response, ensure that all required components are present and in the correct format. I will list the expected components of the response:\n"
            f"- Title: <Provide title here>\n"
            f"- Content: <Provide the content of the post here>\n"
            f"- Tags: <Provide tags here, separated by commas>\n"
            f"- SEO Title: <Provide an SEO optimized title here>\n"
            f"- Focus Keyphrase: <Provide a focus keyphrase here>\n"
            f"- Slug: <Provide SEO-friendly slug here>\n"
            f"- Meta Description: <Provide a meta description here>"
        )
    }
    logging.debug(f"Created personal blog prompts for theme '{theme}'.")
    return [system_prompt, user_prompt]

# Function to generate an article using the Perplexity.ai API
def generate_article(prompts, perplexity_api_key):
    payload = {
        "model": "llama-3.1-sonar-small-128k-online",
        "messages": prompts,
        "max_tokens": 2500,
        "temperature": 0.7,
        "top_p": 0.9,
        "stream": False
    }
    headers = {
        'Authorization': f'Bearer {perplexity_api_key}',
        'Content-Type': 'application/json'
    }
    logging.debug(f"Sending request to Perplexity.ai API with payload: {payload}")
    try:
        response = requests.post('https://api.perplexity.ai/chat/completions', headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            logging.debug(f"Received response from Perplexity.ai API: {data}")
            # Check if the response structure is as expected
            if 'choices' in data and len(data['choices']) > 0 and 'message' in data['choices'][0] and 'content' in data['choices'][0]['message']:
                return 'valid', data['choices'][0]['message']['content']
            else:
                logging.error(f"Unexpected response structure: {data}")
                return 'invalid_structure', None
        else:
            logging.error(f"Error in API request: {response.status_code} - {response.text}")
            return 'api_error', None
    except Exception as e:
        logging.error(f"Exception during API request: {e}")
        return 'api_error', None

# Function to extract post data from the generated article content
# def extract_post_data(final_content):
#     """
#     Извлекает данные поста из сгенерированного контента.

#     Args:
#         final_content (str): Текст, сгенерированный моделью, содержащий данные поста.

#     Returns:
#         dict: Словарь с ключами 'title', 'content', 'tags', 'seo_title',
#               'meta_description', 'focus_keyphrase', 'slug'.
#               Возвращает None в случае ошибки.
#     """
#     data = {}
#     try:
#         # Регулярные выражения для извлечения данных
#         patterns = {
#             'title': r'###\s*[Tt]itle\s*:\s*(.+)',
#             'content': r'###\s*[Cc]ontent\s*:\s*([\s\S]+?)\n###\s*[Ss]EO-datas\s*:',
#             'tags': r'-\s*\*\*Tags:\*\*\s*(.+)',
#             'seo_title': r'-\s*\*\*SEO Title:\*\*\s*(.+)',
#             'meta_description': r'-\s*\*\*Meta Description:\*\*\s*(.+)',
#             'focus_keyphrase': r'-\s*\*\*Focus Keyphrase:\*\*\s*(.+)',
#             'slug': r'-\s*\*\*Slug:\*\*\s*(.+)'
#         }

#         # Извлечение Title
#         title_match = re.search(patterns['title'], final_content, re.IGNORECASE)
#         if title_match:
#             data['title'] = title_match.group(1).strip()
#         else:
#             logging.warning("Не удалось найти 'title' в сгенерированном контенте.")

#         # Извлечение Content
#         content_match = re.search(patterns['content'], final_content, re.IGNORECASE | re.DOTALL)
#         if content_match:
#             data['content'] = content_match.group(1).strip()
#         else:
#             logging.warning("Не удалось найти 'content' в сгенерированном контенте.")

#         # Извлечение Tags
#         tags_match = re.search(patterns['tags'], final_content, re.IGNORECASE)
#         if tags_match:
#             data['tags'] = tags_match.group(1).strip()
#         else:
#             logging.warning("Не удалось найти 'tags' в сгенерированном контенте.")

#         # Извлечение SEO Title
#         seo_title_match = re.search(patterns['seo_title'], final_content, re.IGNORECASE)
#         if seo_title_match:
#             data['seo_title'] = seo_title_match.group(1).strip()
#         else:
#             logging.warning("Не удалось найти 'seo_title' в сгенерированном контенте.")

#         # Извлечение Meta Description
#         meta_description_match = re.search(patterns['meta_description'], final_content, re.IGNORECASE)
#         if meta_description_match:
#             data['meta_description'] = meta_description_match.group(1).strip()
#         else:
#             logging.warning("Не удалось найти 'meta_description' в сгенерированном контенте.")

#         # Извлечение Focus Keyphrase
#         focus_keyphrase_match = re.search(patterns['focus_keyphrase'], final_content, re.IGNORECASE)
#         if focus_keyphrase_match:
#             data['focus_keyphrase'] = focus_keyphrase_match.group(1).strip()
#         else:
#             logging.warning("Не удалось найти 'focus_keyphrase' в сгенерированном контенте.")

#         # Извлечение Slug
#         slug_match = re.search(patterns['slug'], final_content, re.IGNORECASE)
#         if slug_match:
#             data['slug'] = slug_match.group(1).strip()
#         else:
#             logging.warning("Не удалось найти 'slug' в сгенерированном контенте.")

#         # Проверка наличия всех необходимых полей
#         required_fields = ['title', 'content', 'tags', 'seo_title', 'meta_description', 'focus_keyphrase', 'slug']
#         missing_fields = [field for field in required_fields if not data.get(field)]
#         if missing_fields:
#             logging.error(f"Отсутствуют необходимые поля: {missing_fields}")
#             return None

#         logging.debug(f"Extracted post data: {data}")
#         return data

#     except Exception as e:
#         logging.error(f"Error extracting post data: {e}")
#         return None

def extract_post_data(final_content):
    data = {}
    try:
        # Разбор содержимого ответа модели
        # content = generated_article['choices'][0]['message']['content']
        # logging.debug(f"Извлекаем данные из финльного контента: {final_content}")

#         # 1 Используем регулярные выражения для извлечения данных
#         # Используем регулярные выражения, чтобы соответствовать форматам с $$ и без них
#         # title_match = re.search(r'(?:###\s*(?:\$\$)?[tT]itle(?:\$\$)?|####\s*[tT]itle)\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # content_match = re.search(r'(?:###\s*(?:\$\$)?[cC]ontent(?:\$\$)?|####\s*[cC]ontent)\s*:\s*(.+?)(?=\n###|\n####|\Z)', final_content, re.DOTALL | re.IGNORECASE)
#         # tags_match = re.search(r'(?:###\s*(?:\$\$)?[tT]ags(?:\$\$)?|####\s*[tT]ags)\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # seo_title_match = re.search(r'(?:###\s*(?:\$\$)?[sS]EO [tT]itle(?:\$\$)?|####\s*[sS]EO [tT]itle)\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # meta_description_match = re.search(r'(?:###\s*(?:\$\$)?[mM]eta [dD]escription(?:\$\$)?|####\s*[mM]eta [dD]escription)\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # focus_keyphrase_match = re.search(r'(?:###\s*(?:\$\$)?[fF]ocus [kK]eyphrase(?:\$\$)?|####\s*[fF]ocus [kK]eyphrase)\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # slug_match = re.search(r'(?:###\s*(?:\$\$)?[sS]lug(?:\$\$)?|####\s*[sS]lug)\s*:\s*(.+)', final_content, re.IGNORECASE)

        title_match = re.search(r'(?:[tT]itle)\s*:\s*(.+)', final_content, re.IGNORECASE)
        content_match = re.search(r'(?:[cC]ontent)\s*:\s*(.+?)(?=\n###\s|\Z)', final_content, re.DOTALL | re.IGNORECASE)
        tags_match = re.search(r'(?:[tT]ags)\s*:\s*(.+)', final_content, re.IGNORECASE)
        seo_title_match = re.search(r'(?:[sS]EO [tT]itle)\s*:\s*(.+)', final_content, re.IGNORECASE)
        meta_description_match = re.search(r'(?:[mM]eta [dD]escription)\s*:\s*(.+)', final_content, re.IGNORECASE)
        focus_keyphrase_match = re.search(r'(?:[fF]ocus [kK]eyphrase)\s*:\s*(.+)', final_content, re.IGNORECASE)
        slug_match = re.search(r'(?:[sS]lug)\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # 2 Используем регулярные выражения, которые более устойчивы к форматированию
#         # title_match = re.search(r'\$\$[tT]itle\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
#         # content_match = re.search(r'\$\$[cC]ontent\$\$\s*[:\-]*\s*(.*?)\s*(?=\n*\$\$|$)', final_content, re.DOTALL)
#         # tags_match = re.search(r'\$\$[tT]ags\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
#         # seo_title_match = re.search(r'\$\$[sS]EO [tT]itle\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
#         # focus_keyphrase_match = re.search(r'\$\$[fF]ocus [kK]eyphrase\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
#         # slug_match = re.search(r'\$\$[sS]lug\$\$\s*[:\-]*\s*(.*?)\n+', final_content, re.DOTALL)
#         # seo_metadesc_match = re.search(r'\$\$[mM]eta [dD]escription\$\$\s*[:\-]*\s*(.*?)\s*(?=\n*\$\$|$)', final_content, re.DOTALL)

#         # 3 Используем регулярные выражения, которые более устойчивы к форматированию
#         # title_match = re.search(r'\$\$Title\$\$\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # content_match = re.search(r'\$\$Content\$\$\s*:\s*(.+?)(?=\$\$|\Z)', final_content, re.DOTALL | re.IGNORECASE)
#         # tags_match = re.search(r'\$\$Tags\$\$\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # seo_title_match = re.search(r'\$\$SEO Title\$\$\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # meta_description_match = re.search(r'\$\$Meta Description\$\$\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # focus_keyphrase_match = re.search(r'\$\$Focus Keyphrase\$\$\s*:\s*(.+)', final_content, re.IGNORECASE)
#         # slug_match = re.search(r'\$\$Slug\$\$\s*:\s*(.+)', final_content, re.IGNORECASE)

        # Извлечение данных
        data['title'] = title_match.group(1).strip() if title_match else None
        data['content'] = content_match.group(1).strip() if content_match else None
        data['tags'] = tags_match.group(1).strip() if tags_match else None
        data['seo_title'] = seo_title_match.group(1).strip() if seo_title_match else None
        data['meta_description'] = meta_description_match.group(1).strip() if meta_description_match else None
        data['focus_keyphrase'] = focus_keyphrase_match.group(1).strip() if focus_keyphrase_match else None
        data['slug'] = slug_match.group(1).strip() if slug_match else None

        logging.debug(f"Extracted post data: {data}")
        return data
    except Exception as e:
        logging.error(f"Error extracting post data: {e}")
        return None


# Function to extract keywords from content
def extract_keywords(content):
    # Simple keyword extraction by removing stop words and selecting unique words
    words = re.findall(r'\b\w+\b', content.lower())
    stop_words = set(['the', 'and', 'is', 'in', 'to', 'of', 'a', 'for', 'on', 'with', 'as', 'by', 'at', 'from'])
    keywords = list(set(words) - stop_words)
    keywords = [word for word in keywords if len(word) > 2]  # Remove very short words
    return keywords[:10]  # Return top 10 keywords

# Function to retrieve published articles from the database
def get_published_articles(conn, exclude_id=None):
    cursor = conn.cursor()
    try:
        if exclude_id:
            cursor.execute("SELECT title, seo_slug FROM posts WHERE id != %s AND status = 'publish'", (exclude_id,))
        else:
            cursor.execute("SELECT title, seo_slug FROM posts WHERE status = 'publish'")
        articles = cursor.fetchall()
        published_articles = [{'title': row[0], 'slug': row[1]} for row in articles]
        logging.debug(f"Retrieved {len(published_articles)} published articles for cross-linking.")
        return published_articles
    except Exception as e:
        logging.error(f"Error fetching published articles: {e}")
        return []

# Function to insert cross-links into the article content
def insert_cross_links(content, conn, exclude_post_id=None):
    published_articles = get_published_articles(conn, exclude_id=exclude_post_id)
    
    for article in published_articles:
        title = article['title']
        slug = article['slug']
        link = f'https://miatennispro.com/{slug}/'
        content = re.sub(rf'\b{re.escape(title)}\b', f'<a href="{link}">{title}</a>', content)
    
    logging.debug("Inserted cross-links into content.")
    return content

# Function to get image URL from Pixabay based on a query
def get_image_url(query, pixabay_api_key, conn, source='pixabay'):
    cursor = conn.cursor()
    try:
        # Check if the image already exists in the database
        cursor.execute("SELECT image_url FROM images WHERE query = %s", (query,))
        result = cursor.fetchone()
        if result:
            logging.debug("Image URL retrieved from database.")
            return result[0]
        else:
            # Properly URL-encode the query
            encoded_query = urllib.parse.quote(query)
            # Make a request to Pixabay
            url = f"https://pixabay.com/api/?key={pixabay_api_key}&q={encoded_query}&image_type=photo&per_page=3&safesearch=true&order=popular"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data.get('hits'):
                    image_url = data['hits'][0]['webformatURL']
                    logging.debug(f"Retrieved image URL: {image_url}")
                    # Save the image URL to the database
                    cursor.execute(
                        "INSERT INTO images (query, image_url, source, timestamp) VALUES (%s, %s, %s, NOW())",
                        (query, image_url, source)
                    )
                    conn.commit()
                    return image_url
                else:
                    logging.debug("No image found for the query.")
                    return None
            else:
                logging.error(f"Error fetching image from Pixabay: {response.status_code}")
                return None
    except Exception as e:
        logging.error(f"Exception while fetching image from Pixabay: {e}")
        return None

# Function to save image information to the database
def save_image_info(conn, post_id, image_url, alt_text):
    cursor = conn.cursor()
    try:
        query = """
        INSERT INTO post_images (post_id, image_url, alt_text)
        VALUES (%s, %s, %s)
        """
        cursor.execute(query, (post_id, image_url, alt_text))
        conn.commit()
        logging.info(f"Saved image info for post ID {post_id}.")
    except Exception as e:
        logging.error(f"Error saving image info for post ID {post_id}: {e}")
        conn.rollback()

# Function to integrate images into the article content
def integrate_images_into_content(content, theme, pixabay_api_key, conn, post_id):
    image_url = get_image_url(theme, pixabay_api_key, conn)
    if image_url:
        image_html = f'<img src="{image_url}" alt="{theme}" />'
        content_with_image = content.replace('{{IMAGE}}', image_html)
        logging.debug("Integrated image into content.")
        # Save image info
        save_image_info(conn, post_id, image_url, theme)
        return content_with_image
    else:
        content_without_placeholder = content.replace('{{IMAGE}}', '')
        return content_without_placeholder

# Function to save the generated post to the database
def save_post_to_database(conn, post_data, category_id, theme_id):
    cursor = conn.cursor()
    try:
        query = """
        INSERT INTO posts (
            title, content, status, tags, scheduled_date, category_id, category_name, news_id, seo_title, seo_metadesc, seo_focuskw, seo_slug, keywords
        ) VALUES (
            %(title)s, %(content)s, %(status)s, %(tags)s, %(scheduled_date)s, %(category_id)s,
            (SELECT category_name FROM category_wp_id WHERE category_id = %(category_id)s),
            %(news_id)s, %(seo_title)s, %(meta_description)s, %(focus_keyphrase)s, %(slug)s, %(keywords)s
        ) RETURNING id
        """
        # Set additional values for saving
        post_data['status'] = 'pre-Draft'
        post_data['scheduled_date'] = datetime.now() + timedelta(days=1)
        post_data['category_id'] = category_id
        post_data['news_id'] = theme_id
        post_data['keywords'] = extract_keywords(post_data['content'])

        cursor.execute(query, post_data)
        post_id = cursor.fetchone()[0]
        conn.commit()
        logging.info(f"Saved post '{post_data['title']}' to database with ID {post_id}.")
        return post_id
    except Exception as e:
        logging.error(f"Error saving post '{post_data['title']}': {e}")
        conn.rollback()
        return None

# Function to retrieve the required number of themes for a category
def get_required_themes_for_category(conn, category_id):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT required_themes FROM category_theme_settings WHERE category_id = %s", (category_id,))
        result = cursor.fetchone()
        required_themes = result[0] if result else 5  # Default to 5 if no record found
        logging.debug(f"Required themes for category {category_id}: {required_themes}")
        return required_themes
    except Exception as e:
        logging.error(f"Error fetching required themes for category {category_id}: {e}")
        return 5

# Function to retrieve themes that need articles generated
def get_themes_to_generate_articles(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT id, category_id, theme, keywords, description
            FROM blog_post_theme
            WHERE id NOT IN (SELECT news_id FROM posts)
        """)
        themes = cursor.fetchall()
        logging.debug(f"Retrieved {len(themes)} themes to generate articles.")
        return themes
    except Exception as e:
        logging.error(f"Error fetching themes for article generation: {e}")
        return []

# Main function to orchestrate the script
def main():
    configure_logging()
    logging.info("Script started.")

    conn = get_db_connection()
    if not conn:
        logging.error("Database connection failed. Exiting script.")
        return

    try:
        perplexity_api_key_local = perplexity_api_key  # Ensure using the correct API key
        pixabay_api_key_local = pixabay_api_key

        categories = get_categories()

        for category in categories:
            category_id = category['category_id']
            category_name = category['category_name']
            logging.info(f"Processing category '{category_name}' (ID: {category_id}).")

            required_new_themes = get_required_themes_for_category(conn, category_id)
            new_themes_added = 0
            max_attempts = 5
            attempts = 0

            themes_already_added = set()

            while new_themes_added < required_new_themes and attempts < max_attempts:
                existing_themes = get_existing_themes(conn, category_id)
                keywords = get_keywords_for_category(category_name)

                prompts = create_theme_prompt(category_name, existing_themes, keywords)
                generated_content = generate_new_themes(prompts, perplexity_api_key_local)
                
                if generated_content:
                    new_themes = parse_generated_ideas(generated_content)
                    unique_new_themes = []
                    for idea in new_themes:
                        theme_title = idea['title']
                        if (theme_title not in existing_themes) and (theme_title not in themes_already_added):
                            unique_new_themes.append(idea)
                            themes_already_added.add(theme_title)
                        else:
                            logging.warning(f"Duplicate theme '{theme_title}' detected. Skipping.")
                    for idea in unique_new_themes:
                        success = save_theme_to_db(conn, category_id, idea['title'], idea['description'], keywords)
                        if success:
                            new_themes_added += 1
                            if new_themes_added >= required_new_themes:
                                break
                    if new_themes_added < required_new_themes:
                        logging.info(f"Need {required_new_themes - new_themes_added} more themes for category '{category_name}'. Retrying...")
                else:
                    logging.error("Failed to generate new themes.")
                    break
                attempts += 1

            if new_themes_added < required_new_themes:
                logging.warning(f"Could not generate the required number of new themes for category '{category_name}'. Only {new_themes_added} were added.")
            else:
                logging.info(f"Successfully added {new_themes_added} new themes for category '{category_name}'.")

        # Fetch themes that need articles generated
        themes = get_themes_to_generate_articles(conn)

        for theme in themes:
            theme_id, category_id, theme_title, keywords_str, description = theme
            logging.info(f"Generating article for theme '{theme_title}' (ID: {theme_id}).")
            keywords = [kw.strip() for kw in keywords_str.split(',')]

            # Use different prompts based on the category
            if category_id == 20:  # My Personal Blog
                prompts = create_personal_blog_prompt(theme_title, description, keywords)
            else:
                prompts = create_article_prompt(theme_title, description, keywords, theme_id, conn)

            # Request to API to generate article
            status, final_content = generate_article(prompts, perplexity_api_key_local)

            if status == 'valid' and final_content:
                if not isinstance(final_content, str):
                    logging.error(f"Expected a string, but got {type(final_content)}: {final_content}")
                    continue

                post_data = extract_post_data(final_content)

                if post_data and all(post_data.values()):
                    # Save post and get its ID
                    post_id = save_post_to_database(conn, post_data, category_id, theme_id)
                    if not post_id:
                        logging.error(f"Failed to save post '{post_data['title']}' to database. Skipping.")
                        continue

                    # Integrate images and save image info
                    post_data['content'] = integrate_images_into_content(post_data['content'], theme_title, pixabay_api_key_local, conn, post_id)
                    # Insert cross-links
                    post_data['content'] = insert_cross_links(post_data['content'], conn, exclude_post_id=post_id)
                    # Update post with new content
                    cursor = conn.cursor()
                    cursor.execute("UPDATE posts SET content = %s WHERE id = %s", (post_data['content'], post_id))
                    conn.commit()
                    logging.info(f"Updated post ID {post_id} with integrated images and cross-links.")
                else:
                    logging.error("Post data is incomplete or missing. Skipping saving to database.")
            elif status == 'invalid_structure':
                logging.error(f"Unexpected structure in API response: {final_content}")
            else:
                logging.error("Failed to generate article.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info("Script finished successfully.")

# Entry point of the script
if __name__ == "__main__":
    main()
