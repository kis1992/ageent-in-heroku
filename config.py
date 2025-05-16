# config.py

class Config:
    SECRET_KEY = 'dev'
    # Настройки Celery по умолчанию
    #CELERY_BROKER_URL = os.environ.get('REDIS_URL', None)
    #CELERY_RESULT_BACKEND = os.environ.get('REDIS_URL', None)
   