import signal
import sys

from django.core.management.base import BaseCommand

from django_debezium_sink.handler import DebeziumHandler


class Command(BaseCommand):
    help = 'Listen to debezium events'

    def add_arguments(self, parser):
        parser.add_argument('app', type=str)

    def handle(self, *args, **options):
        handler = DebeziumHandler()
        handler.listen()

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
