# assuming you already have amqplib, graypy and pika (if not: sudo pip install graypy pika amqplib)

import pika
from graypy import rabbitmq
import logging
import os

LOG_RABBITMQ_USER = 'ls1'
LOG_RABBITMQ_PASSWORD = 'hs7fmSTsw2fupTz6Lm8gbGr7X'
LOG_RABBITMQ_HOST = 'ls-mq.nyc.3top.com'
LOG_RABBITMQ_EXCHANGE = 'logstash_nyc_sys_exchange'
LOG_RABBITMQ_QUEUE = 'logstash_nyc_sys_queue'

def start_logging():
    logging.getLogger('pika').setLevel(logging.ERROR)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host = LOG_RABBITMQ_HOST, port = 5672, virtual_host = '/', 
        credentials = pika.credentials.PlainCredentials(username = LOG_RABBITMQ_USER,
                                                        password = LOG_RABBITMQ_PASSWORD)))
    
    channel = connection.channel()

    try:
        channel.exchange_declare(exchange = LOG_RABBITMQ_EXCHANGE,
                         type = 'fanout', durable = True)
        channel.queue_declare(queue = LOG_RABBITMQ_QUEUE, durable = True)
        channel.queue_bind(queue = LOG_RABBITMQ_QUEUE, exchange = LOG_RABBITMQ_EXCHANGE,
                           routing_key = '')
    except:
        pass

    connection.close()

def get_handlers(logger_name, filename):
    start_logging()
    handler_logstash = rabbitmq.GELFRabbitHandler('amqp://' +  LOG_RABBITMQ_USER +  ':' +  LOG_RABBITMQ_PASSWORD +  '@' +  LOG_RABBITMQ_HOST +  '/%2F', LOG_RABBITMQ_EXCHANGE)
    log_filename = filename
    my_formatter = logging.Formatter('%(levelname)s %(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    handler_file = logging.FileHandler(log_filename)
    handler_file.setFormatter(my_formatter)
    os.environ['logger_name'] = logger_name
    return (handler_logstash, handler_file)
