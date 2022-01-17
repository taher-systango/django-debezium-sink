import importlib
import json
import pprint
import signal
import sys

from confluent_kafka import Consumer, KafkaException
from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand

from django_debezium_sink.signals import debezium_updates


class Command(BaseCommand):
    help = 'Listen to debezium events'

    def add_arguments(self, parser):
        parser.add_argument('app', type=str)

    def handle(self, *args, **options):
        topics = self.create_topics()
        topics = self.validate_topics(topics)
        consumer = self.create_consumer()
        consumer.subscribe(topics)
        self.listen(consumer)

    def listen(self, consumer):
        try:
            self.log(f'Creating Kafka consumer at {settings.DDS_BROKER_HOST}:{settings.DDS_BROKER_PORT}', 'success')
            self.log(f'Consuming topics: {self.topics}', 'success')
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    if not msg.value():
                        continue
                    payload = self.extract_payload(msg.value())
                    # pprint.pprint(payload)
                    model = self.get_model_from_payload(payload)
                    debezium_updates.send(sender=model, payload=payload)
        finally:
            consumer.close()

    def create_consumer(self, group=None):
        if not group:
            group = settings.DDS_KAFKA_CONSUMER_GROUP
        broker = f'{settings.DDS_BROKER_HOST}:{settings.DDS_BROKER_PORT}'
        conf = {'bootstrap.servers': broker, 'group.id': group, 'auto.offset.reset': 'latest'}
        return Consumer(conf)

    def get_model_from_payload(self, payload):
        table_name = payload.get('source').get('table')
        return self._models[table_name]

    def extract_payload(self, value):
        return json.loads(value).get('payload')

    def create_topics(self):
        server_name = settings.DDS_DEBEZIUM_CONNECTOR_SERVER_NAME
        schema = settings.DDS_DATABASE_SCHEMA
        prefix = server_name
        if schema:
            prefix = f'{prefix}.{schema}.'
        models = self.parse_models()
        self.topics = [prefix + model._meta.app_label + '_' + model._meta.model_name for model in models]
        return self.topics

    def validate_topics(self, topics):
        consumer = self.create_consumer('django-debezium-sink-validate')
        server_topics = consumer.list_topics().topics.keys()
        if len(set(topics) - set(server_topics)) > 0:
            self.log(f'Ignoring topics not found on server: {set(topics) - set(server_topics)}', 'warn')
        return list(set(topics) & set(server_topics))

    def parse_models(self):
        self._models = {}
        if len(settings.DDS_MODELS) == 0:
            raise ImproperlyConfigured('DDS_MODELS setting not found')
        for item in settings.DDS_MODELS:
            try:
                item_list = item.split('.')
                model = getattr(importlib.import_module('.'.join(item_list[0:-1:])), item_list[-1])
                self._models[model._meta.db_table] = model
            except Exception as e:
                print(e)
                self.log(f'Cannot load model "{item}"', 'error')
        return self._models.values()

    def log(self, msg, style):
        msg = f'django-debezium-sink: {msg}'
        if style == 'error':
            self.stdout.write(self.style.ERROR(msg))
        elif style == 'warn':
            self.stdout.write(self.style.WARNING(msg))
        elif style == 'success':
            self.stdout.write(self.style.SUCCESS(msg))


def int_singal(sig, frame):
    print('Stopped successfully')
    sys.exit(0)


signal.signal(signal.SIGINT, int_singal)
