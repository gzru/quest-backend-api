import os.path
import yaml
import logging


class Config(object):

    # Aerospike
    AEROSPIKE_HOSTS = [
        '165.227.86.99:3000'
    ]
    AEROSPIKE_MAX_RECORD_SIZE = 128000
    AEROSPIKE_CONNECTION_TIMEOUT_MS = 10000
    AEROSPIKE_RW_TIMEOUT_MS = 500
    AEROSPIKE_LUA_USER_PATH = 'lua/'

    # Aerospike content
    AEROSPIKE_NS_USERS = 'test'
    AEROSPIKE_NS_SIGNS = 'test'
    AEROSPIKE_NS_SIGNS_SEARCH_IDX = 'test'
    AEROSPIKE_SIGNS_SEARCH_IDX_GLOBAL_SET = 'search_index_global'
    AEROSPIKE_SIGNS_SEARCH_IDX_PRIVATE_SET = 'search_index_private'
    AEROSPIKE_SIGNS_SEARCH_IDX_GEO_BIN = 'location'

    # S3
    S3_ENDPOINT_URL = 'http://10.136.60.75:9000'
    S3_ACCESS_KEY_ID = 'quest'
    S3_SECRET_ACCESS_KEY = 'theiWos1aiph2phiepoQu5gaiCoox1th'

    # Twilio
    TWILIO_ACCOUNT_SID = 'ACa706bcada5933c32c8924f2902e54542'
    TWILIO_AUTH_TOKEN = 'a98d12be39a9374ef83d045e529cc1b4'
    TWILIO_API_KEY = 'SKf4811fbc8d9d0103797f28185b03bc67'
    TWILIO_API_SECRET = 'X7mKjNNNPy2VQ80JMcfyaaGWtPF0wpWX'
    TWILIO_CHAT_SERVICE_SID = 'IS32555b78ade742d89abc6fd18b6c32ee'
    TWILIO_PUSH_CREDENTIAL_SID = 'CR5776d4fff23e9bf0d85389af5c763eb3'

    # Matching
    MATCHER_HOSTS = [
        'matcher.quest.aiarlabs.com'
    ]
    MATCHER_TIMEOUT_SEC = 10

    # Elasticsearch
    ELASTICSEARCH_ENABLED = False
    ELASTICSEARCH_HOSTS = []
    ELASTICSEARCH_TIMEOUT_SEC = 0.1


    @staticmethod
    def initialize(filepath):
        if not os.path.isfile(filepath):
            logging.info('There is no file %s', filepath)
            return True

        try:
            with open(filepath) as stream:
                config_data = yaml.load(stream)
        except Exception as ex:
            logging.error('Can\'t parse yaml config %s, %s', filepath, ex)
            return False

        Config.AEROSPIKE_HOSTS = config_data.get('aerospike_hosts', Config.AEROSPIKE_HOSTS)
        Config.AEROSPIKE_MAX_RECORD_SIZE = config_data.get('aerospike_max_record_size', Config.AEROSPIKE_MAX_RECORD_SIZE)
        Config.AEROSPIKE_CONNECTION_TIMEOUT_MS = config_data.get('aerospike_connection_timeout_ms', Config.AEROSPIKE_CONNECTION_TIMEOUT_MS)
        Config.AEROSPIKE_RW_TIMEOUT_MS = config_data.get('aerospike_rw_timeout_ms', Config.AEROSPIKE_RW_TIMEOUT_MS)

        Config.AEROSPIKE_NS_USERS = config_data.get('aerospike_ns_users', Config.AEROSPIKE_NS_USERS)
        Config.AEROSPIKE_NS_SIGNS = config_data.get('aerospike_ns_signs', Config.AEROSPIKE_NS_SIGNS)
        Config.AEROSPIKE_NS_SIGNS_SEARCH_IDX = config_data.get('aerospike_ns_signs_search_idx', Config.AEROSPIKE_NS_SIGNS_SEARCH_IDX)

        Config.S3_ENDPOINT_URL = config_data.get('s3_endpoint_url', Config.S3_ENDPOINT_URL)
        Config.S3_ACCESS_KEY_ID = config_data.get('s3_access_key_id', Config.S3_ACCESS_KEY_ID)
        Config.S3_SECRET_ACCESS_KEY = config_data.get('s3_secret_access_key', Config.S3_SECRET_ACCESS_KEY)

        Config.MATCHER_HOSTS = config_data.get('matcher_hosts', Config.MATCHER_HOSTS)
        Config.MATCHER_TIMEOUT_SEC = config_data.get('matcher_timeout_sec', Config.MATCHER_TIMEOUT_SEC)

        Config.ELASTICSEARCH_ENABLED = config_data.get('elasticsearch_enabled', Config.ELASTICSEARCH_ENABLED)
        Config.ELASTICSEARCH_HOSTS = config_data.get('elasticsearch_hosts', Config.ELASTICSEARCH_HOSTS)
        Config.ELASTICSEARCH_TIMEOUT_SEC = config_data.get('elasticsearch_timeout_sec', Config.ELASTICSEARCH_TIMEOUT_SEC)

        return True


if __name__ == "__main__":
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    Config.initialize('/tmp/config.yaml')
    print Config.AEROSPIKE_HOSTS

