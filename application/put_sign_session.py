from query import Query, BadQuery
import hashlib
import json


class PutSignQuery(Query):

    def __init__(self):
        self.user_id = None
        self.latitude = None
        self.longitude = None
        self.radius = None
        self.time_to_live = None
        self.features = None
        self.meta_blob = None
        self.object_blob = None
        self.image_blob = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_optional_int64(tree, 'user_id')
        self.latitude = self._get_optional_float64(tree, 'latitude')
        self.longitude = self._get_optional_float64(tree, 'longitude')
        self.radius = self._get_optional_float64(tree, 'radius')
        self.time_to_live = self._get_optional_int64(tree, 'time_to_live')
        self.features = self._parse_features(tree)
        self.meta_blob = self._get_optional_blob(tree, 'meta_blob')
        self.object_blob = self._get_optional_blob(tree, 'object_blob')
        self.image_blob = self._get_optional_blob(tree, 'image_blob')

    def _parse_features(self, tree):
        features = tree.get('features')
        if not features:
            return None

        if not isinstance(features, list):
            raise BadQuery('"features" have bad format')

        for value in features:
            self._check_float('features_value', value)

        return features


class PutSignSession:

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._kafka_connector = global_context.kafka_connector
        self._namespace = 'test'
        self._info_set = 'sign_info'
        self._meta_set = 'sign_meta'
        self._object_set = 'sign_object'
        self._image_set = 'sign_image'

    def parse_query(self, data):
        self._query = PutSignQuery()
        self._query.parse(data)

    def execute(self):
        sign_id = self._gen_sign_id(self._query)

        self._check_duplicate(sign_id)
        self._put_meta(sign_id, self._query)
        self._put_object(sign_id, self._query)
        self._put_image(sign_id, self._query)
        self._put_info(sign_id, self._query)

        return json.dumps({ 'sign_id': sign_id })

    def _gen_sign_id(self, query):
        hasher = hashlib.md5()
        def _update(value):
            if value:
                hasher.update(str(value))
        _update(query.user_id)
        _update(query.latitude)
        _update(query.longitude)
        _update(query.object_blob)
        return int(hasher.hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF

    def _check_duplicate(self, sign_id):
        info_key = (self._namespace, self._info_set, str(sign_id))
        if self._aerospike_connector.check_exists(info_key):
            raise Exception('Sign already exists')

    def _put_meta(self, sign_id, query):
        if not query.meta_blob:
            return
        meta_key = (self._namespace, self._meta_set, str(sign_id))
        if not self._aerospike_connector.put_data(meta_key, query.meta_blob):
            raise Exception('Can\'t save meta blob')

    def _put_object(self, sign_id, query):
        if not query.object_blob:
            return
        object_key = (self._namespace, self._object_set, str(sign_id))
        if not self._aerospike_connector.put_data(object_key, query.object_blob):
            raise Exception('Can\'t save object blob')

    def _put_image(self, sign_id, query):
        if not query.image_blob:
            return
        image_key = (self._namespace, self._image_set, str(sign_id))
        if not self._aerospike_connector.put_data(image_key, query.image_blob):
            raise Exception('Can\'t save image blob')

    def _put_info(self, sign_id, query):
        info_key = (self._namespace, self._info_set, str(sign_id))
        info = {
            'user_id': query.user_id,
            'latitude': query.latitude,
            'longitude': query.longitude,
            'radius': query.radius,
            'time_to_live': query.time_to_live,
            'features': query.features
        }
        if not self._aerospike_connector.put_data(info_key, json.dumps(info)):
            raise Exception('Can\'t save info')

