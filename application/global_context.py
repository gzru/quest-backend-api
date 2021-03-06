from config import Config
from connectors.aerospike_connector import AerospikeConnector
from connectors.twilio_connector import TwilioConnector
from connectors.s3connector import S3Connector
from connectors.elasticsearch_connector import ElasticsearchConnector
from avatar_generator import AvatarGenerator
from core.access_rules import AccessRules


class GlobalContext:

    def __init__(self):
        self.access_rules = None
        self.aerospike_connector = None
        self.avatar_generator = None
        self.twilio_connector = None
        self.s3connector = None
        self.elasticsearch_connector = None

    def initialize(self):
        if not Config.initialize('/etc/quest/config.yaml'):
            raise Exception('Can\'t read config')

        self.aerospike_connector = AerospikeConnector(max_record_size=Config.AEROSPIKE_MAX_RECORD_SIZE, \
                                                      rw_timeout_ms=Config.AEROSPIKE_RW_TIMEOUT_MS, \
                                                      connection_timeout_ms=Config.AEROSPIKE_CONNECTION_TIMEOUT_MS)
        if not self.aerospike_connector.connect(Config.AEROSPIKE_HOSTS, Config.AEROSPIKE_LUA_USER_PATH):
            raise Exception('Can\'t connect to aerospike')

        self.access_rules = AccessRules(self)
        self.avatar_generator = AvatarGenerator('data/avatar_palette.png', 'data/SanFranciscoDisplay-Regular.ttf')
        self.twilio_connector = TwilioConnector()

        self.s3connector = S3Connector(Config.S3_ENDPOINT_URL, \
                                       Config.S3_ACCESS_KEY_ID, \
                                       Config.S3_SECRET_ACCESS_KEY)

        self.elasticsearch_connector = ElasticsearchConnector(Config.ELASTICSEARCH_HOSTS, \
                                                              Config.ELASTICSEARCH_USERS_INDEX, \
                                                              Config.ELASTICSEARCH_TIMEOUT_SEC, \
                                                              enabled=Config.ELASTICSEARCH_ENABLED)

