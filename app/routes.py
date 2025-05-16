from flask import Blueprint, request, jsonify, render_template
from app.tasks import (
    process_user_messages, 
    get_conversation_history, 
    check_status_conversation, 
    message_to_manager, 
    redis_client,
    clean_url
)
import os
import re
from celery.result import AsyncResult
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Регулярное выражение для поиска ссылок на Instagram
INSTAGRAM_REGEX = re.compile(r'https?://(www\.)?instagram\.com/\S+')

def contains_instagram_link(text):
    """Проверяет наличие ссылок на Instagram в тексте"""
    return bool(INSTAGRAM_REGEX.search(text))

# Создаем Blueprint
bp = Blueprint('routes', __name__)

@bp.route('/history', methods=['GET'])
def hello_history():
    """Маршрут для просмотра истории сообщений"""
    user_id = request.args.get('userid')
    if not user_id:
        return "Не указан ID пользователя", 400
        
    data = get_conversation_history(user_id, True)
    return render_template('history3.html', data=data)

@bp.route('/webhook', methods=['POST'])
def webhook():
    """Webhook для обработки входящих сообщений"""
    try:
        data = request.get_json()
        channel_id = os.getenv("channal_id")
        
        # Проверяем структуру данных
        if not data or 'messages' not in data:
            logger.info(f"1**In webhook data hadnt messages or not data***")
            return jsonify({"status": "invalid_data"}), 200
            
        messages = data['messages']
        if not messages:
            logger.info(f"2**In webhook data hadnt messages or***")
            return jsonify({"status": "no_messages"}), 200
            
        # Получаем первое сообщение
        first_message = messages[0]
        
        # Проверяем, что сообщение от пользователя (не от бота)
        if 'authorName' in first_message:
            logger.info(f"3**In webhook data had authorName***")
            return jsonify({"status": "bot_message_ignored"}), 200
            
        # Проверяем, что сообщение из нужного канала
        if first_message.get('channelId') != channel_id:
            logger.info(f"4**Chanal id is not same***")
            return jsonify({"status": "wrong_channel"}), 200
            
        user_id = first_message.get('chatId')
        
        # Проверяем статус разговора
        if not check_status_conversation(user_id):
            logger.info(f"5**Users conversation status are closed***")
            return jsonify({"status": "conversation_closed"}), 200
            
        # Обрабатываем не текстовые сообщения
        if first_message.get('type') != "text":
            logger.info(f"6**Message type is not text***")
            message_to_manager(first_message)
            return jsonify({"status": "media_forwarded_to_manager"}), 200
            
        # Проверяем наличие ссылок на Instagram
        message_text = first_message['text']
        if contains_instagram_link(message_text):
            logger.info(f"7**Message contains instagram link***")
            message_to_manager(first_message, False)
            return jsonify({"status": "instagram_link_forwarded"}), 200
            
        # Формируем ключ для Redis
        user_key = f"user_{clean_url(os.environ.get('bot_url'))}_{user_id}"
        
        try:
            # Используем pipeline для группировки операций Redis
            with redis_client.pipeline() as pipe:
                # Добавляем сообщение в список и устанавливаем срок жизни
                pipe.rpush(f"{user_key}_messages", message_text)
                pipe.expire(f"{user_key}_messages", 90)
                logger.info(f"8**Message text: {message_text}***")
                # Получаем текущий task_id
                pipe.get(f"{user_key}_task_id")
                results = pipe.execute()
                
                current_task_id = results[2]  # Результат команды get
                
                # Если есть активная задача
                if current_task_id:
                    logger.info(f"***Current task: {current_task_id}***")
                    # Устанавливаем новый срок жизни для task_id
                    redis_client.expire(f"{user_key}_task_id", 90)
                    
                    # Отменяем текущую задачу, если она еще не завершена
                    task = AsyncResult(current_task_id)
                    if not task.ready():
                        logger.info(f"***Task revoke: {current_task_id}***")
                        task.revoke(terminate=True)
                
                # Создаем новую асинхронную задачу
                new_task = process_user_messages.apply_async(
                    args=[user_id, first_message],
                    expires=35  # Задача истекает через 150 секунд
                )
                
                # Устанавливаем новый task_id с TTL
                logger.info(f"***New task: {new_task.id}***")
                redis_client.setex(f"{user_key}_task_id", 150, new_task.id)
                
                return jsonify({"status": "message_queued", "task_id": new_task.id}), 200
                
        except Exception as e:
            # Логируем ошибку
            print(f"Ошибка при обработке сообщения: {e}")
            logger.error(f"---Ошибка при обработке сообщения: {e}---")
            return jsonify({"status": "error", "message": str(e)}), 500
            
    except Exception as e:
        print(f"Необработанная ошибка в webhook: {e}")
        logger.error(f"---Необработанная ошибка в webhook: {e}---")

        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/start')
def index():
    """Корневой маршрут"""
    return "Hello, World!"

# Можно добавить маршрут для проверки состояния Redis
@bp.route('/health')
def health_check():
    """Проверка состояния Redis"""
    try:
        # Проверяем подключение к Redis
        info = redis_client.info()
        connected_clients = info.get('connected_clients', 0)
        total_connections_received = info.get('total_connections_received', 0)
        
        return jsonify({
            "status": "ok",
            "redis": {
                "connected": True,
                "connected_clients": connected_clients,
                "total_connections_received": total_connections_received
            }
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Redis error: {str(e)}"
        }), 500