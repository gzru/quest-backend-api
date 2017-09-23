from aerospike_connector import AerospikeConnector
from kafka_connector import KafkaConnector


class GlobalContext:

    def __init__(self):
        self.aerospike_connector = None
        self.kafka_connector = None

    def initialize(self):
        self.aerospike_connector = AerospikeConnector()
        if not self.aerospike_connector.connect(['165.227.86.99']):
            raise Exception('Can\'t connect to aerospike')

        self.kafka_connector = KafkaConnector()
        if not self.kafka_connector.connect(['165.227.94.62']):
            raise Exception('Can\'t connect to kafka')

