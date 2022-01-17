from django.conf import settings

DEFAULTS = {
    'DDS_BROKER_HOST': 'localhost',
    'DDS_BROKER_PORT': '9092',
    'DDS_DATABASE_SCHEMA': 'public',
    'DDS_KAFKA_CONSUMER_GROUP': 'django-debezium-sink',
    'DDS_DEBEZIUM_CONNECTOR_SERVER_NAME': 'server',
    'DDS_MODELS': []
}

for key in DEFAULTS:
    if not hasattr(settings, key):
        setattr(settings, key, DEFAULTS[key])
