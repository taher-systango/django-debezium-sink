import signal
import sys

from django.core.management.base import BaseCommand

from django_debezium_sink.handler import DebeziumHandler


class Command(BaseCommand):
    help = 'Listen to debezium events'

    def handle(self, *args, **options):
        handler = DebeziumHandler()
        handler.listen()


def int_signal(sig, frame):
    print('Stopped successfully')
    sys.exit(0)


signal.signal(signal.SIGINT, int_signal)
