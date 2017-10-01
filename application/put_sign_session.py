from query import Query, BadQuery
from sign_engine import SignsEngine, SignInfo
import hashlib
import json
import time


class PutSignQuery(Query):

    def __init__(self):
        self.user_id = None
        self.latitude = None
        self.longitude = None
        self.radius = None
        self.time_to_live = None
        self.timestamp = None
        self.features = None
        self.meta_blob = None
        self.object_blob = None
        self.image_blob = None
        self.preview_blob = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_required_int64(tree, 'user_id')
        self.latitude = self._get_required_float64(tree, 'latitude')
        self.longitude = self._get_required_float64(tree, 'longitude')
        self.radius = self._get_required_float64(tree, 'radius')
        self.features = self._parse_features(tree)
        # Blobs will be required in a while
        self.meta_blob = self._get_optional_blob(tree, 'meta_blob')
        self.object_blob = self._get_optional_blob(tree, 'object_blob')
        self.image_blob = self._get_optional_blob(tree, 'image_blob')
        self.preview_blob = self._get_optional_blob(tree, 'preview_blob')

        self.time_to_live = self._get_optional_int64(tree, 'time_to_live')
        if self.time_to_live == None:
            self.time_to_live = 0

        self.timestamp = self._get_optional_int64(tree, 'timestamp')
        if self.timestamp == None:
            self.timestamp = time.time()

    def _parse_features(self, tree):
        features = self._get_required(tree, 'features')

        if not isinstance(features, list):
            raise BadQuery('"features" have bad format')

        for value in features:
            self._check_float('features_value', value)

        return features


class PutSignSession:

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = PutSignQuery()
        self._query.parse(data)

    def execute(self):
        info = SignInfo()
        info.user_id = self._query.user_id
        info.latitude = self._query.latitude
        info.longitude = self._query.longitude
        info.radius = self._query.radius
        info.time_to_live = self._query.time_to_live
        info.timestamp = self._query.timestamp

        sign_id = self._sign_engine.put_sign(info, \
                                    self._query.features, \
                                    self._query.meta_blob, \
                                    self._query.object_blob, \
                                    self._query.image_blob, \
                                    self._query.preview_blob)

        return json.dumps({'success': True, 'sign_id': sign_id})

