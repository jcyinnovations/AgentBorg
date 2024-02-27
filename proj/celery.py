from celery import Celery, bootsteps
from kombu import Exchange, Queue

# Setup Celery to use Redis
#url = "amqp://admin:ccst4s0r@localhost:5672/"
url = 'redis://localhost:6379/0'
app = Celery('rl_inference', broker=url, include=['proj.tasks'])

if __name__ == '__main__':
    app.start()

