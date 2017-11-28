from config import Config
from aerospike_connector import AerospikeConnector
from kafka_connector import KafkaConnector
from searcher_connector import SearcherConnector
from twilio_connector import TwilioConnector
from s3connector import S3Connector
from avatar_generator import AvatarGenerator


class GlobalContext:

    def __init__(self):
        self.aerospike_connector = None
        self.kafka_connector = None
        self.searcher_connector = None
        self.twilio_connector = None
        self.s3connector = None

    def initialize(self):
        if not Config.initialize('/tmp/config.yaml'):
            raise Exception('Can\'t read config')

        self.aerospike_connector = AerospikeConnector(max_record_size=Config.AEROSPIKE_MAX_RECORD_SIZE, \
                                                      rw_timeout_ms=Config.AEROSPIKE_RW_TIMEOUT_MS, \
                                                      connection_timeout_ms=Config.AEROSPIKE_CONNECTION_TIMEOUT_MS)
        if not self.aerospike_connector.connect(Config.AEROSPIKE_HOSTS, Config.AEROSPIKE_LUA_USER_PATH):
            raise Exception('Can\'t connect to aerospike')

        #self.kafka_connector = KafkaConnector(request_timeout_ms=settings.KAFKA_REQUEST_TIMEOUT_MS, \
        #                                      send_message_timeout_sec=settings.KAFKA_SEND_MESSAGE_TIMEOUT_SEC)
        #if not self.kafka_connector.connect(settings.KAFKA_HOSTS):
        #    raise Exception('Can\'t connect to kafka')

        self.avatar_generator = AvatarGenerator('data/avatar_palette.png', 'data/SanFranciscoDisplay-Regular.ttf')

        self.searcher_connector = SearcherConnector()
        self.twilio_connector = TwilioConnector()

        self.s3connector = S3Connector(Config.S3_ENDPOINT_URL, \
                                       Config.S3_ACCESS_KEY_ID, \
                                       Config.S3_SECRET_ACCESS_KEY)

