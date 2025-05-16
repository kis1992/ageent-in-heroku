from app import create_app

_, celery = create_app()

if __name__ == '__main__':
    celery.worker_main()