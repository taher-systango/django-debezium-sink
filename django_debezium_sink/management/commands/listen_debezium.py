import json
import signal
import sys

from confluent_kafka import Consumer, KafkaException
from django.apps import apps
from django.conf import settings
from django.core.management import CommandError
from django.core.management.base import BaseCommand

from django_debezium_sink.signals import debezium_updates


class Command(BaseCommand):
    help = 'Listen to debezium events'

    def add_arguments(self, parser):
        parser.add_argument('app', type=str)

    def handle(self, *args, **options):
        self.app = options['app']
        self.validate_app()
        topics = self.create_topics()
        topics = self.validate_topics(topics)
        consumer = self.create_consumer()
        consumer.subscribe(topics)
        self.listen(consumer)

    def listen(self, consumer):
        try:
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
                    model = self.get_model(payload)
                    debezium_updates.send(sender=model, payload=payload)
        finally:
            consumer.close()

    def create_consumer(self, group=None):
        if not group:
            group = settings.DDS_KAFKA_CONSUMER_GROUP
        broker = f'{settings.DDS_BROKER_HOST}:{settings.DDS_BROKER_PORT}'
        conf = {'bootstrap.servers': broker, 'group.id': group, 'auto.offset.reset': 'latest'}
        return Consumer(conf)

    def get_model(self, payload):
        table_name = payload.get('source').get('table').split(f'{self.app}_')[1]
        return apps.get_model(self.app, table_name)

    def extract_payload(self, value):
        return json.loads(value).get('payload')

    def create_topics(self):
        server_name = settings.DDS_DEBEZIUM_CONNECTOR_SERVER_NAME
        schema = settings.DDS_DATABASE_SCHEMA
        prefix = server_name
        if schema:
            prefix = f'{prefix}.{schema}.{self.app}_'
        models = apps.all_models[self.app].keys()
        topics = [prefix + model for model in models]
        return topics

    def validate_topics(self, topics):
        consumer = self.create_consumer('django-debezium-sink-validate')
        server_topics = consumer.list_topics().topics.keys()
        if len(set(topics) - set(server_topics)) > 0:
            self.log(f'Ignoring topics not found on server: {set(topics) - set(server_topics)}')
        return list(set(topics) & set(server_topics))

    def validate_app(self):
        if self.app not in apps.all_models.keys():
            raise CommandError(f"No installed app with label '{self.app}'")

    def log(self, msg):
        msg = f'django-debezium-sink: {msg}'
        self.stdout.write(self.style.WARNING(msg))


def int_singal(sig, frame):
    print('Stopped successfully')
    sys.exit(0)


signal.signal(signal.SIGINT, int_singal)
