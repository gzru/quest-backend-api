from config import Config
from aerospike_connector import AerospikeConnector


if not Config.initialize('/etc/quest/config.yaml'):
    raise Exception('Can\'t read config')

connector = AerospikeConnector(max_record_size=Config.AEROSPIKE_MAX_RECORD_SIZE, \
                               rw_timeout_ms=Config.AEROSPIKE_RW_TIMEOUT_MS, \
                               connection_timeout_ms=Config.AEROSPIKE_CONNECTION_TIMEOUT_MS)

if not connector.connect(Config.AEROSPIKE_HOSTS, Config.AEROSPIKE_LUA_USER_PATH):
    raise Exception('Can\'t connect to aerospike')


# Create search index
connector._client.index_geo2dsphere_create(Config.AEROSPIKE_NS_SIGNS_SEARCH_IDX, \
                                           Config.AEROSPIKE_SIGNS_SEARCH_IDX_GLOBAL_SET, \
                                           Config.AEROSPIKE_SIGNS_SEARCH_IDX_GEO_BIN, \
                                           Config.AEROSPIKE_SIGNS_SEARCH_IDX_GLOBAL_SET + '_geo_index')

connector._client.index_geo2dsphere_create(Config.AEROSPIKE_NS_SIGNS_SEARCH_IDX, \
                                           Config.AEROSPIKE_SIGNS_SEARCH_IDX_PRIVATE_SET, \
                                           Config.AEROSPIKE_SIGNS_SEARCH_IDX_GEO_BIN, \
                                           Config.AEROSPIKE_SIGNS_SEARCH_IDX_PRIVATE_SET + '_geo_index')

# Upload udf script
connector._client.udf_put('../application/lua/search.lua')

connector._client.close()

