import aerospike


AEROSPIKE_CONFIG = {
    'hosts': [
        ('localhost', 3000)
    ]
}

NAMESPACE = 'test'
SEARCH_INDEX_GLOBAL_SET = 'global_search_index'
SEARCH_INDEX_PRIVATE_SET = 'private_search_index'
SEARCH_INDEX_GEO_BIN = 'location'


client = aerospike.client(AEROSPIKE_CONFIG).connect()

# Create search index
client.index_geo2dsphere_create(NAMESPACE, SEARCH_INDEX_GLOBAL_SET, SEARCH_INDEX_GEO_BIN, SEARCH_INDEX_GLOBAL_SET + '_geo_index')
client.index_geo2dsphere_create(NAMESPACE, SEARCH_INDEX_PRIVATE_SET, SEARCH_INDEX_GEO_BIN, SEARCH_INDEX_PRIVATE_SET + '_geo_index')

# Upload udf script
client.udf_put('search.lua')

client.close()

