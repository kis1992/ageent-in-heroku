from flask import Flask
from celery import Celery
import os
import ssl

redis_url = os.environ.get('REDIS_URL')

def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config')

    # Инициализация Celery
    celery = Celery(
        app.import_name,
    )
    celery.conf.update(
        app.config,
        broker_pool_limit=5,
        
        # Ограничение пула для бэкенда результатов
        result_backend_pool_limit=5,
        
        # Установка тайм-аута для задач (по умолчанию)
        task_time_limit=120,
        
        # Мягкий тайм-аут (предупреждение)
        task_soft_time_limit=100,
        
        # Сохранять результаты задач на 1 час
        result_expires=360,
        
        # Транспортные опции
        broker_transport_options={
            'visibility_timeout': 360,  # 1 час
            'fanout_prefix': True,
            'fanout_patterns': True
        },
        
        # Настройки для бэкенда результатов
        redis_max_connections=20,
        
        # Автоматически удалять задачи из очереди после выполнения
        task_ignore_result=True,
        
        # Сжатие сообщений для экономии памяти
        task_compression='gzip',
        
        # Настройки префетчинга (сколько задач брать за раз)
        worker_prefetch_multiplier=1,
        
        # Периодические задачи
        beat_schedule={
            'cleanup-stale-locks': {
                'task': 'app.tasks.cleanup_stale_locks',
                'schedule': 100.0,  # Каждые 5 минут
            },
        }
    )
    celery.conf.broker_url = redis_url
    celery.conf.result_backend = redis_url
    celery.conf.broker_use_ssl = {
    'ssl_cert_reqs': ssl.CERT_NONE,
    'ssl_ca_certs': None,
    'socket_connect_timeout': 10,
    'socket_timeout': 10,
    'retry_on_timeout': True
    }
    celery.conf.redis_backend_use_ssl = {
        'ssl_cert_reqs': ssl.CERT_NONE,
        'ssl_ca_certs': None,
        'socket_connect_timeout': 10,
        'socket_timeout': 10,
        'retry_on_timeout': True
    }

    celery.autodiscover_tasks(['app.tasks'])
    
    # Регистрация задач Celery
    from app import tasks
    tasks.celery = celery

    # Регистрация маршрутов
    from app import routes
    app.register_blueprint(routes.bp)

    return app, celery