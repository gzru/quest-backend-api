from config import Config
from connectors.elasticsearch_connector import ElasticsearchConnector
import sys


if not Config.initialize('/etc/quest/config.yaml'):
    raise Exception('Can\'t read config')

if Config.ELASTICSEARCH_ENABLED == False:
    print 'Elasticsearch is disabled, do nothing'
    sys.exit()

elasticsearch_connector = ElasticsearchConnector(Config.ELASTICSEARCH_HOSTS, \
                                                 Config.ELASTICSEARCH_USERS_INDEX, \
                                                 Config.ELASTICSEARCH_TIMEOUT_SEC, \
                                                 enabled=Config.ELASTICSEARCH_ENABLED)

with open('pre-install/elasticsearch_index_settings.json', 'r') as content_file:
    content = content_file.read()

elasticsearch_connector.create_index(content)

