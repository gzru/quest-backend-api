import aerospike


class Entry(object):

    def __init__(self):
        self.user_id = None
        self.sign_id = None
        self.latitude = None
        self.longitude = None
        self.features = []
        self.is_private = True


class SearchEntry(object):

    def __init__(self):
        self.sign_id = None
        self.rank = None
        self.distance = None


class SearchIndex(object):

    def __init__(self, connector):
        self._aerospike_connector = connector
        self._namespace = 'test'
        self._global_set = 'global_search_index'
        self._private_set = 'private_search_index'

    def add_sign(self, entry):
        location = aerospike.GeoJSON(
            {
                'type': 'Point',
                'coordinates': [entry.latitude, entry.longitude]
            }
        )
        record = {
            'user_id': entry.user_id,
            'sign_id': entry.sign_id,
            'location': location,
            'features': entry.features,
            'is_private': int(entry.is_private)
        }
        self._aerospike_connector.put_bins((self._namespace, self._global_set, str(entry.sign_id)), record)

    def add_private_access(self, user_id, sign_id):
        global_record = self._aerospike_connector.get_bins((self._namespace, self._global_set, str(sign_id)))
        private_record = {
            'user_id': user_id,
            'sign_id': sign_id,
            'location': global_record['location'],
            'features': global_record['features']
        }
        pk = '{}:{}'.format(user_id, sign_id)
        self._aerospike_connector.put_bins((self._namespace, self._private_set, pk), private_record)

    def search_region(self, user_id, lat1, lon1, lat2, lon2, callback):
        region = aerospike.GeoJSON(
            { 'type': "Polygon",
              'coordinates': [[
                    [lat1, lon1],
                    [lat1, lon2],
                    [lat2, lon2],
                    [lat2, lon1],
                    [lat1, lon1]
                ]]
            }
        ).dumps()
        # Public
        query = self._aerospike_connector._client.query(self._namespace, self._global_set)
        predicate = aerospike.predicates.geo_within_geojson_region('location', region)
        query.where(predicate)
        query.apply('search', 'apply_access_filter', [user_id])
        query.foreach(callback)
        # Private
        query = self._aerospike_connector._client.query(self._namespace, self._private_set)
        predicate = aerospike.predicates.geo_within_geojson_region('location', region)
        query.where(predicate)
        query.apply('search', 'apply_access_filter', [user_id])
        query.foreach(callback)

    def search_nearest(self, user_id, latitude, longitude, radius, callback):
        # Public
        query = self._aerospike_connector._client.query(self._namespace, self._global_set)
        predicate = aerospike.predicates.geo_within_radius('location', latitude, longitude, radius)
        query.where(predicate)
        query.apply('search', 'apply_access_filter', [user_id])
        query.foreach(callback)
        # Private
        query = self._aerospike_connector._client.query(self._namespace, self._private_set)
        predicate = aerospike.predicates.geo_within_radius('location', latitude, longitude, radius)
        query.where(predicate)
        query.apply('search', 'apply_access_filter', [user_id])
        query.foreach(callback)

    def search_by_fea(self, user_id, latitude, longitude, radius, features, callback):
        # Public
        query = self._aerospike_connector._client.query(self._namespace, self._global_set)
        predicate = aerospike.predicates.geo_within_radius('location', latitude, longitude, radius)
        query.where(predicate)
        query.apply('search', 'apply_ranking', [user_id, features])
        query.foreach(callback)
        # Private
        query = self._aerospike_connector._client.query(self._namespace, self._private_set)
        predicate = aerospike.predicates.geo_within_radius('location', latitude, longitude, radius)
        query.where(predicate)
        query.apply('search', 'apply_ranking', [user_id, features])
        query.foreach(callback)


if __name__ == "__main__":
    import aerospike_connector

    connector = aerospike_connector.AerospikeConnector(128000, 10, 100)
    connector.connect(['localhost'])

    si = SearchIndex(connector)

    e1 = Entry()
    e1.user_id = 10001
    e1.sign_id = 20001
    e1.latitude = 1
    e1.longitude = 1
    e1.features = [1, 2, 3]
    si.add_sign(e1)

    e2 = Entry()
    e2.user_id = 10002
    e2.sign_id = 20002
    e2.latitude = 1
    e2.longitude = 1
    e2.features = [3, 2, 1]
    e2.is_private = False
    si.add_sign(e2)

    e3 = Entry()
    e3.user_id = 10002
    e3.sign_id = 20003
    e3.latitude = 1
    e3.longitude = 1
    e3.features = [1, 4, 9]
    si.add_sign(e3)
    si.add_private_access(10001, 20003)

    def format(entry):
        print entry

    si.search_region(10001, 1.5, 1.5, 0.5, 0.5, format)

