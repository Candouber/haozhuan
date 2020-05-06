# -*- coding: utf-8 -*-
import os
import json
import asyncio
import logging
import datetime
import traceback
from threading import Thread
from kafka import KafkaProducer
from urllib.parse import urlparse
from logging.handlers import TimedRotatingFileHandler


class KafkaLoggingHandler(logging.Handler):

    def __init__(self, topic):
        logging.Handler.__init__(self)
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=['10.10.132.114:9092',
                                                         '10.10.60.6:9092',
                                                         '10.10.178.160:9092',
                                                         '10.10.81.185:9092',
                                                         '10.10.184.158:9092',
                                                         '10.10.97.62:9092'])

        self.LOOP = asyncio.get_event_loop()
        LOOP_THREAD = Thread(target=self.run_loop, args=(self.LOOP,))
        LOOP_THREAD.start()

    def run_loop(self, loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def emit(self, record):
        asyncio.run_coroutine_threadsafe(self.write(record.msg), self.LOOP)

    async def write(self, message):
        try:
            message = json.loads(message)
            log_template = {
                'source': 'B7',
                'platform': message.get('platform'),
                'task': message.get('task'),
                'enter_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'event': message.get('event'),
                'function': message.get('function'),
                'url': message.get('url'),
                'proxy': message.get('proxy'),
                'status_code': message.get('status_code', 0),
                'event_flag': message.get('event_flag'),
                'content': json.dumps(message.get('content')),
                'error_flag': message.get('error_flag'),
                'error_message': message.get('error_message'),
                'data': json.dumps(message.get('data')),
                'website_id': message.get('website_id', 0),
                'duration': message.get('duration', 0),
                'reserve1': message.get('reserve1'),
                'reserve2': message.get('reserve2'),
                'reserve3': message.get('reserve3')
            }
            log_template = json.dumps(log_template)
            # print(log_template)
            self.producer.send(self.topic, bytes(
                log_template, encoding='UTF-8'))
            self.producer.flush()
        except Exception as e:
            print(traceback.format_exc())

    def close(self):
        self.producer.close()


CRAWLER_LOGGER = logging.getLogger('crawl_coupon_logger')
CRAWLER_LOGGER.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s[%(levelname)s] : %(message)s')

TOPIC = 'SpiderApiServerLog_T1'
kh = KafkaLoggingHandler(TOPIC)
kh.setLevel(logging.INFO)
kh.setFormatter(formatter)
CRAWLER_LOGGER.addHandler(kh)


def get_logger(file_name, level=logging.DEBUG, when="midnight", back_count=7):
    logger = logging.getLogger(file_name)
    logger.setLevel(level)
    log_path = "/data/log/jingdou"

    log_file_path = os.path.join(log_path, file_name)
    formatter = logging.Formatter(
        '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    # ch = logging.StreamHandler()
    # ch.setLevel(level)
    fh = logging.handlers.TimedRotatingFileHandler(
        filename=log_file_path, when=when, backupCount=back_count, encoding='utf-8')
    fh.setLevel(level)
    fh.suffix = "%Y-%m-%d"
    fh.setFormatter(formatter)
    # ch.setFormatter(formatter)
    logger.addHandler(fh)
    # logger.addHandler(ch)
    return logger


logger = get_logger("jingdou.log")


def write_log(message):
    CRAWLER_LOGGER.info(json.dumps(message))
    logger.info(json.dumps(message, ensure_ascii=False))
