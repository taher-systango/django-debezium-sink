import importlib
import json
import logging
import os
from dataclasses import dataclass
from multiprocessing import Pool

import colorlog
from confluent_kafka import KafkaException, Consumer
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from django_debezium_sink.signals import debezium_updates

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))

logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class DebeziumHandler:
    """
    Main class for handling all django-debezium-sink functionality.
    """
    def __init__(self):
        self._models = []
        self.topics = self.validate_topics(self.generate_topics())
        self.consumer = self.create_consumer()
        self.consumer.subscribe(self.topics)
        cpu_count = os.cpu_count() or 2
        self.pool = Pool(processes=cpu_count - 1)

    def listen(self):
        """
        Start listening to Kafka using the settings defined at settings.py
        :return:
        """
        try:
            self.log(f'Creating Kafka consumer at {settings.DDS_BROKER_HOST}:{settings.DDS_BROKER_PORT}', 'success')
            self.log(f'Consuming topics: {self.topics}', 'success')
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    if not msg.value():
                        continue
                    self.process_message(msg)
        finally:
            self.consumer.close()
            self.pool.terminate()

    @staticmethod
    def send_message(model, payload):
        """
        Call debezium_updates django signal to send payload to all the subscribers of that model
        :param model:
        :param payload:
        :return:
        """
        debezium_updates.send(sender=model, payload=payload)

    def generate_result(self, payload):
        """
        Convert dict and nested dicts to dataclass
        :param payload:
        :return:
        """
        result = Result()
        for key in payload:
            if type(payload[key]) == dict:
                value = self.generate_result(payload.get(key))
            else:
                value = payload.get(key)
            setattr(result, key, value)
        return result

    def process_message(self, msg):
        """
        Data parsing and message sending
        :param msg:
        :return:
        """
        payload = self.extract_payload(msg.value())
        # pprint.pprint(payload)
        model = self.get_model_from_payload(payload)
        result = self.generate_result(payload)
        self.pool.apply_async(DebeziumHandler.send_message, args=(model, result))
        # DebeziumHandler.send_message(model, result)

    def get_model_from_payload(self, payload):
        """
        Extract database table name from the payload and get associated model from instance
        :param payload:
        :return:
        """
        table_name = payload.get('source').get('table')
        return self._models[table_name]

    def extract_payload(self, value: str):
        return json.loads(value).get('payload')

    def generate_topics(self):
        """
        Generate topic names using the information provided in settings
        :return:
        """
        server_name = settings.DDS_DEBEZIUM_CONNECTOR_SERVER_NAME
        schema = settings.DDS_DATABASE_SCHEMA
        prefix = server_name
        if schema:
            prefix = f'{prefix}.{schema}.'
        models = self.parse_models()
        return [prefix + model._meta.app_label + '_' + model._meta.model_name for model in models]

    def parse_models(self):
        """
        Resolve and parse settings.DDS_MODELS into self._models
        :return:
        """
        self._models = {}
        if len(settings.DDS_MODELS) == 0:
            raise ImproperlyConfigured('DDS_MODELS setting not found or found an empty iterable')
        for item in settings.DDS_MODELS:
            try:
                item_list = item.split('.')
                model = getattr(importlib.import_module('.'.join(item_list[0:-1:])), item_list[-1])
                self._models[model._meta.db_table] = model
            except Exception as e:
                self.log(e, 'error')
                self.log(f'Cannot load model "{item}"', 'error')
        return self._models.values()

    def validate_topics(self, topics: list):
        """
        Verify local topics with topics present in Kafka
        :param topics:
        :return:
        """
        consumer = self.create_consumer('DDS-validate-topics')
        server_topics = consumer.list_topics().topics.keys()
        if len(set(topics) - set(server_topics)) > 0:
            self.log(f'Ignoring topics not found on server: {set(topics) - set(server_topics)}', 'warn')
        return list(set(topics) & set(server_topics))

    def create_consumer(self, group=None):
        """
        Create Kafka consumer
        :param group:
        :return:
        """
        if not group:
            group = settings.DDS_KAFKA_CONSUMER_GROUP
        broker = f'{settings.DDS_BROKER_HOST}:{settings.DDS_BROKER_PORT}'
        conf = {'bootstrap.servers': broker, 'group.id': group, 'auto.offset.reset': 'latest'}
        return Consumer(conf)

    def log(self, msg, style):
        if style == 'success':
            logger.info(msg)
        elif style == 'warn':
            logger.warning(msg)
        elif style == 'error':
            logger.error(msg)


@dataclass
class Result:
    """
    This gets populated dynamically with payload
    """
    pass
