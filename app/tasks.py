from celery import shared_task
import redis
from openai import OpenAI
import requests
import time
import sqlite3
from threading import Lock
import os
import ssl
import re
from redis import ConnectionPool
import logging
import json
import threading

logger = logging.getLogger(__name__)
url_database = "https://ailiner.kz/history"
# Файловый замок для предотвращения конкурентных записей
file_lock = threading.Lock()

class JSONStorage:
    def __init__(self, file_path=None):
        if file_path is None:
            # Используем директорию /tmp, доступную на Heroku
            self.file_path = '/tmp/conversations.json'
        else:
            self.file_path = file_path
        
        # Создаем файл, если он не существует
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Создаем файл с пустым JSON-объектом, если он не существует"""
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as f:
                json.dump({}, f)
    
    def load_data(self):
        """Загружает все данные из JSON-файла"""
        max_retries = 3
        retry_delay = 0.2
        
        for attempt in range(max_retries):
            try:
                with file_lock:
                    with open(self.file_path, 'r') as f:
                        return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning(f"Ошибка при чтении JSON ({attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    # Если файл поврежден или не существует, создаем заново
                    self._ensure_file_exists()
                else:
                    logger.error("Не удалось загрузить данные после нескольких попыток")
                    return {}
    
    def save_data(self, data):
        """Сохраняет все данные в JSON-файл"""
        max_retries = 3
        retry_delay = 0.2
        
        for attempt in range(max_retries):
            try:
                with file_lock:
                    with open(self.file_path, 'w') as f:
                        json.dump(data, f)
                return True
            except Exception as e:
                logger.warning(f"Ошибка при записи JSON ({attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Не удалось сохранить данные после нескольких попыток")
                    return False

# Теперь реализуем те же функции, что и раньше
def get_conversation_history(user_id, history=False):
    """Получает историю разговора пользователя"""
    try:
        response = requests.post(url_database,json={"user_id":f"{user_id}"})
        data = response.json()
        thread_id = data.get("thread_id",None)
        logger.info(f"???Response  thread object -> {thread_id}")
        if thread_id is None or thread_id == "None" or thread_id == "":
            logger.info("Thread ID не найден, создаем новый")
            return None
        
        if history:
            messages = client.beta.threads.messages.list(thread_id=thread_id)
            assistant_reply = extract_role_content(messages, True)
            logger.info(f"История диалога для thread {thread_id}: {assistant_reply}")
            return assistant_reply
        else:
            return thread_id
            
    except Exception as e:
        logger.error(f"Ошибка при получении истории разговора: {e}", exc_info=True)
        return None

def save_conversation_history(user_id, history):
    """Сохраняет историю разговора пользователя"""
    try:
        response = requests.post(url_database,json={"user_id":f"{user_id}","thread_id":f"{history}"})
        data = response.json()
        logger.info("conversation succesful written")
    except Exception as e:
        logger.error(f"Ошибка при сохранении истории разговора: {e}", exc_info=True)

client = OpenAI(api_key=os.environ.get('OPENAI_KEY'))
assistant = client.beta.assistants.retrieve(os.environ.get('ASSISTANT_KEY'))

redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

# Создаем пул соединений для Redis
ssl_params = {
    'ssl_cert_reqs': ssl.CERT_NONE,
    'socket_connect_timeout': 10,
    'socket_timeout': 10,
    'socket_keepalive': True,
    'health_check_interval': 30,
    'retry_on_timeout': True
} if redis_url.startswith('rediss://') else {}

# Создаем общий пул соединений с ограничением максимального количества
redis_pool = ConnectionPool.from_url(
    url=redis_url,
    max_connections=10,  # Ограничиваем количество соединений
    decode_responses=True,  # Автоматически декодируем ответы из байтов в строки
    **ssl_params
)

# Создаем клиента Redis, использующего пул соединений
redis_client = redis.Redis(connection_pool=redis_pool)

#redis_client = redis.from_url(os.environ.get('REDIS_URL'))
def clean_url(url: str) -> str:
    return re.sub(r'https://|\.herokuapp\.com/', '', url)

# Вспомогательная функция для выполнения операций Redis с автоматической обработкой ошибок
def redis_operation(operation_func, retry_count=3, retry_delay=1):
    """Выполняет операцию с Redis с повторными попытками"""
    for attempt in range(retry_count):
        try:
            return operation_func()
        except redis.exceptions.ConnectionError as e:
            print(f"Ошибка подключения Redis (попытка {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
            else:
                print("Исчерпаны все попытки подключения к Redis")
                raise
        except Exception as e:
            print(f"Ошибка при работе с Redis: {e}")
            raise

@shared_task
def process_user_messages(user_id, data):
    """Обрабатывает сообщения пользователя из Redis и отправляет ответ"""
    time.sleep(9)
    prefix = f"user_{clean_url(os.environ.get('bot_url'))}_{user_id}"
    
    def _get_and_clear_messages():
        # Получаем сообщения и сразу очищаем данные в Redis (используя pipeline для атомарности)
        with redis_client.pipeline() as pipe:
            pipe.lrange(f"{prefix}_messages", 0, -1)
            pipe.delete(f"{prefix}_messages")
            pipe.delete(f"{prefix}_task_id")
            results = pipe.execute()
            return results[0]  # Первый результат - это список сообщений
    
    try:
        # Получаем и обрабатываем сообщения
        messages = redis_operation(_get_and_clear_messages)
        
        # Объединяем сообщения в один текст (они уже декодированы благодаря decode_responses=True)
        combined_messages = " ".join(messages)
        
        # Формируем общий ответ и обновляем данные для webhook
        data['text'] = combined_messages
        
        # Отправляем ответ через webhook, если есть текст
        if data['text'] and not data['text'].isspace():
            logger.info(f"*1*Joined webhook text: {data['text']} ***")
            webhook(data)
            
        print(f"Отправлен ответ пользователю {user_id}: текст длиной {len(combined_messages)} символов")
    except Exception as e:
        logger.error(f"---Ошибка при обработке сообщений пользователя {user_id}: {e}---")
        print(f"Ошибка при обработке сообщений пользователя {user_id}: {e}")

def gpt_input(data_from_bitrix):
    """Обрабатывает запрос через GPT"""
    user_message = data_from_bitrix["text"]
    print(f'User_message: {user_message}')
    user_id = data_from_bitrix["user_id"]
    conversation_history = get_conversation_history(user_id)
    logger.info(f"3**User_message: {user_message} --- and ---tthread {conversation_history} ***")

    if conversation_history is None:
        thread = client.beta.threads.create(
            messages=[{
                "role": "user",
                "content": "Прайс или цена услуги товара описание или характиристика",
                "attachments": [
                    {"file_id": f"{os.environ.get('file_id')}", "tools": [{"type": "file_search"}]}
                ],
            }]
        )
        conversation_history = thread.id 
        save_conversation_history(user_id, conversation_history)

    message = client.beta.threads.messages.create(
        thread_id=conversation_history,
        role="user",
        content=f"{user_message}",
    )

    run = client.beta.threads.runs.create_and_poll(
        thread_id=conversation_history,
        assistant_id=assistant.id
    )

    if run.status == 'completed': 
        messages = client.beta.threads.messages.list(
            thread_id=conversation_history,
        )
        print("completed - - ", messages)
    else:
        print(run.status)
        
    assistant_reply = extract_role_content(messages)
    print(f'gpt response: {assistant_reply}')
    logger.info(f"4**gpt response: {assistant_reply}***")
 
    return assistant_reply

def webhook(first_message, gpt_answer='code_gpt_base'):
    """Отправляет сообщение через webhook"""
    if gpt_answer == 'code_gpt_base':
        gpt_data = {}
        gpt_data['text'] = first_message.get('text')
        gpt_data['user_id'] = first_message.get('chatId')
        gpt_answer = gpt_input(gpt_data)

    json_data = {
        'channelId': first_message.get('channelId'),
        'chatId': first_message.get('chatId'),
        'chatType': 'whatsapp',
        'text': f'{gpt_answer}',
    }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {os.environ.get("wazzap_api_key")}',
    }
    try:
        response = requests.post("https://api.wazzup24.com/v3/message", headers=headers, json=json_data)
        response_data = response.json()
    except Exception as e:
        response_data = e
    return {"message": f"{gpt_answer}", "response_text": f"{response_data}"}

# Контекстный менеджер для безопасной работы с SQLite
class SQLiteConnection:
    def __init__(self, db_name=None, max_retries=5, retry_delay=0.1):
        if db_name is None:
            # Используем абсолютный путь в /tmp, который доступен на Heroku
            self.db_name = '/tmp/conversation.db'
        else:
            self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Убедимся, что таблица существует
        self._ensure_table_exists()
        
    def _ensure_table_exists(self):
        # Создаем таблицу, если она не существует
        conn = sqlite3.connect(self.db_name)
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS conversation_history (
                    user_id TEXT PRIMARY KEY,
                    history TEXT
                )
            ''')
            conn.commit()
        finally:
            conn.close()
            
    def __enter__(self):
        retries = 0
        last_error = None
        
        while retries < self.max_retries:
            try:
                self.conn = sqlite3.connect(self.db_name, timeout=20)
                self.conn.row_factory = sqlite3.Row  # Для удобного доступа к колонкам по имени
                self.cursor = self.conn.cursor()
                return self.cursor
            except sqlite3.OperationalError as e:
                # Обработка ошибки "database is locked"
                last_error = e
                retries += 1
                time.sleep(self.retry_delay)
                
        # Если все попытки неудачны, выбрасываем последнюю ошибку
        raise last_error if last_error else sqlite3.OperationalError("Could not connect to database")
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is not None:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()

def save_user_info(user_account, channel_id):
    """Сохраняет информацию о пользователе"""
    try:
        with SQLiteConnection() as cursor:
            cursor.execute('''
                INSERT OR REPLACE INTO userinfo (user_account, dialog_channel) VALUES (?, ?)
            ''', (f"{user_account}", f"{channel_id}"))
    except Exception as e:
        print(f"Ошибка при сохранении информации о пользователе: {e}")

def check_status_conversation(user_id):
    """Проверяет статус разговора пользователя"""
    try:
        with SQLiteConnection() as cursor:
            cursor.execute('SELECT status FROM conversation_history WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            
        if result is None:
            return True
        return bool(result[0])
    except Exception as e:
        print(f"Ошибка при проверке статуса разговора: {e}")
        return True

def update_status(user_id):
    """Обновляет статус разговора пользователя"""
    try:
        with SQLiteConnection() as cursor:
            cursor.execute('''
                UPDATE conversation_history
                SET status = ?
                WHERE user_id = ?
            ''', (0, user_id))
    except Exception as e:
        print(f"Ошибка при обновлении статуса разговора: {e}")

def extract_role_content(data, history=False):
    """Извлекает содержимое из ответа API"""
    results = []
    for message in data.data:
        role = message.role
        content_text = ""
        for content_block in message.content:
            if content_block.type == "text":
                content_text += content_block.text.value
        results.append({"role": role, "content": content_text})
    
    if history:
        return results
    else:
        return results[0]['content']

def with_lock(client_id, operation_func, *args, **kwargs):
    """Выполняет операцию с блокировкой"""
    lock_key = f"lock:{client_id}"
    lock_acquired = False
    
    try:
        # Пытаемся получить блокировку
        lock_acquired = redis_client.set(lock_key, "locked", nx=True, ex=10)
        
        if lock_acquired:
            # Если блокировка получена, выполняем функцию
            return operation_func(*args, **kwargs)
        else:
            # Если блокировка не получена, ждем и повторяем попытку
            for _ in range(5):  # Пробуем 5 раз
                time.sleep(0.5)
                lock_acquired = redis_client.set(lock_key, "locked", nx=True, ex=10)
                if lock_acquired:
                    return operation_func(*args, **kwargs)
            
            # Если после всех попыток блокировка не получена
            print(f"Невозможно получить блокировку для клиента {client_id}")
            return None
    finally:
        # Гарантированно освобождаем блокировку, если она была получена
        if lock_acquired:
            redis_client.delete(lock_key)

def message_to_manager(first_message, analyzer=True):
    """Отправляет сообщение менеджеру с блокировкой"""
    client_id = first_message.get('chatId')
    
    def _process_message():
        if check_status_conversation(client_id):
            print(f"Status conversation history is -- {check_status_conversation(client_id)}")
            webhook(first_message, gpt_answer=os.environ.get("trigger_words"))
            update_status(client_id)
            
            # Отправляем сообщение менеджеру
            manager_message = first_message.copy()
            manager_message['chatId'] = os.environ.get("admin_phone")
            
            if analyzer:
                message = f'Посмотри медиа файл, {first_message.get("contentUri")} \n а тут переписка - {os.getenv("bot_url")}history?userid={client_id} \n Кстати, а вот и номер клиента +{client_id}'
            else:
                message = f'Посмотри переписку, бот не может ответить \n вот тут переписка - {os.getenv("bot_url")}history?userid={client_id} \n Кстати, вот и номер клиента +{client_id}'
                
            webhook(manager_message, gpt_answer=message)
    
    # Выполняем с блокировкой
    return with_lock(client_id, _process_message)

@shared_task
def cleanup_stale_locks():
    """Очищает устаревшие блокировки"""
    try:
        lock_pattern = "lock:*"
        keys = redis_client.keys(lock_pattern)
        
        for key in keys:
            # Проверка TTL ключа
            ttl = redis_client.ttl(key)
            if ttl < 0:  # Если TTL истек или не установлен
                redis_client.delete(key)
                print(f"Очищена устаревшая блокировка: {key}")
    except Exception as e:
        print(f"Ошибка при очистке устаревших блокировок: {e}")
