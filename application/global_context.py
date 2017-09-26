from aerospike_connector import AerospikeConnector
from kafka_connector import KafkaConnector
import settings


class GlobalContext:

    def __init__(self):
        self.aerospike_connector = None
        self.kafka_connector = None

    def initialize(self):
        self.aerospike_connector = AerospikeConnector(max_record_size=settings.AEROSPIKE_MAX_RECORD_SIZE, \
                                                      rw_timeout_ms=settings.AEROSPIKE_RW_TIMEOUT_MS, \
                                                      connection_timeout_ms=settings.AEROSPIKE_CONNECTION_TIMEOUT_MS)
        if not self.aerospike_connector.connect(settings.AEROSPIKE_HOSTS):
            raise Exception('Can\'t connect to aerospike')

        self.kafka_connector = KafkaConnector(request_timeout_ms=settings.KAFKA_REQUEST_TIMEOUT_MS, \
                                              send_message_timeout_sec=settings.KAFKA_SEND_MESSAGE_TIMEOUT_SEC)
        if not self.kafka_connector.connect(settings.KAFKA_HOSTS):
            raise Exception('Can\'t connect to kafka')
