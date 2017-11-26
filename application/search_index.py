from error import APILogicalError
import aerospike
import logging
import math


class Entry(object):

    def __init__(self):
        self.user_id = None
        self.sign_id = None
        self.latitude = None
        self.longitude = None
        self.features = []
        self.is_private = True


class SearchIndex(object):

    def __init__(self, connector):
        self._aerospike_connector = connector
        self._namespace = 'test'
        self._global_set = 'search_index_global'
        self._private_set = 'search_index_private'

    def add_sign(self, info, features):
        location = aerospike.GeoJSON(
            {
                'type': 'Point',
                'coordinates': [info.longitude, info.latitude]
            }
        )
        # Normailize features vector
        qsum = 0
        for fea in features:
            qsum = qsum + fea * fea
        qsum = math.sqrt(qsum)
        for i in range(len(features)):
            features[i] = features[i] / qsum
        # aerospike udf cant handle int64
        # so use strings instead
        # TODO try to split int64 into pair of int32 bins
        record = {
            'user_id': str(info.user_id),
            'sign_id': str(info.sign_id),
            'location': location,
            'features': features,
            'is_private': int(info.is_private)
        }
        self._aerospike_connector.put_bins((self._namespace, self._global_set, str(info.sign_id)), record)

    def set_sign_privacy(self, sign_id, is_private):
        update = {
            'is_private': int(is_private)
        }
        self._aerospike_connector.put_bins((self._namespace, self._global_set, str(sign_id)), update)

    def remove_sign(self, sign_id):
        self._aerospike_connector.remove((self._namespace, self._global_set, str(sign_id)))

    def add_private_access(self, user_id, sign_id):
        global_record = self._aerospike_connector.get_bins((self._namespace, self._global_set, str(sign_id)))
        if global_record == None:
            raise APILogicalError('Record was not found in global index')
        private_record = {
            'user_id': str(user_id),
            'sign_id': str(sign_id),
            'location': global_record['location'],
            'features': global_record['features']
        }
        pk = '{}:{}'.format(user_id, sign_id)
        self._aerospike_connector.put_bins((self._namespace, self._private_set, pk), private_record)

    def remove_private_access(self, user_id, sign_id):
        pk = '{}:{}'.format(user_id, sign_id)
        self._aerospike_connector.remove((self._namespace, self._private_set, pk))

    def search_region(self, user_id, lat1, lon1, lat2, lon2, callback):
        user_id_str = str(user_id)
        region = aerospike.GeoJSON(
            { 'type': "Polygon",
              'coordinates': [[
                    [lon1, lat1],
                    [lon2, lat1],
                    [lon2, lat2],
                    [lon1, lat2],
                    [lon1, lat1]
                ]]
            }
        ).dumps()
        # Public
        query = self._aerospike_connector._client.query(self._namespace, self._global_set)
        predicate = aerospike.predicates.geo_within_geojson_region('location', region)
        query.where(predicate)
        query.apply('search', 'apply_access_filter', [user_id_str])
        query.foreach(callback)
        # Private
        query = self._aerospike_connector._client.query(self._namespace, self._private_set)
        predicate = aerospike.predicates.geo_within_geojson_region('location', region)
        query.where(predicate)
        query.apply('search', 'apply_access_filter', [user_id_str])
        query.foreach(callback)

    def search_nearest(self, user_id, latitude, longitude, radius, callback):
        user_id_str = str(user_id)
        policy = {
            'timeout': 100,
        }
        # Public
        query = self._aerospike_connector._client.query(self._namespace, self._global_set)
        predicate = aerospike.predicates.geo_within_radius('location', longitude, latitude, radius)
        query.where(predicate)
        query.apply('search', 'apply_access_filter', [user_id_str])
        query.foreach(callback, policy=policy)
        # Private
        query = self._aerospike_connector._client.query(self._namespace, self._private_set)
        predicate = aerospike.predicates.geo_within_radius('location', longitude, latitude, radius)
        query.where(predicate)
        query.apply('search', 'apply_access_filter', [user_id_str])
        query.foreach(callback, policy=policy)

    def search_by_fea(self, user_id, latitude, longitude, radius, features, callback):
        user_id_str = str(user_id)
        # Public
        query = self._aerospike_connector._client.query(self._namespace, self._global_set)
        predicate = aerospike.predicates.geo_within_radius('location', longitude, latitude, radius)
        query.where(predicate)
        query.apply('search', 'apply_ranking', [user_id_str, features])
        query.foreach(callback)
        # Private
        query = self._aerospike_connector._client.query(self._namespace, self._private_set)
        predicate = aerospike.predicates.geo_within_radius('location', longitude, latitude, radius)
        query.where(predicate)
        query.apply('search', 'apply_ranking', [user_id_str, features])
        query.foreach(callback)


if __name__ == "__main__":
    import aerospike_connector

    connector = aerospike_connector.AerospikeConnector(128000, 10, 100)
    connector.connect(['165.227.86.99'])

    si = SearchIndex(connector)

    e1 = Entry()
    e1.user_id = 10001
    e1.sign_id = 20001
    e1.latitude = 1
    e1.longitude = 1
    e1.features = [1, 2, 3]
    si.add_sign(e1, e1.features)

    e2 = Entry()
    e2.user_id = 10002
    e2.sign_id = 20002
    e2.latitude = 1
    e2.longitude = 1
    e2.features = [3, 2, 1]
    e2.is_private = False
    si.add_sign(e2, e2.features)

    e3 = Entry()
    e3.user_id = 10002
    e3.sign_id = 20003
    e3.latitude = 1
    e3.longitude = 1
    e3.features = [1, 4, 9]
    si.add_sign(e3, e3.features)
    si.add_private_access(10001, 20003)

    def format(entry):
        print entry

    si.search_region(10001, 1.5, 1.5, 0.5, 0.5, format)

