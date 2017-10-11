from query import Query, BadQuery
from signs_engine import SignsEngine, SignInfo
from users_engine import UsersEngine
import hashlib
import json
import time
import logging


class PutSignQuery(Query):

    def __init__(self):
        self.user_id = None
        self.latitude = None
        self.longitude = None
        self.radius = None
        self.time_to_live = None
        self.timestamp = None
        self.is_private = None
        self.features = None
        self.meta_blob = None
        self.object_blob = None
        self.image_blob = None
        self.preview_blob = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = int(self._get_required_str(tree, 'user_token'))
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
            self.timestamp = int(time.time())

        self.is_private = self._get_optional_bool(tree, 'is_private')
        if self.is_private == None:
            self.is_private = True

    def _parse_features(self, tree):
        features = self._get_required(tree, 'features')

        if not isinstance(features, list):
            raise BadQuery('"features" have bad format')

        for value in features:
            self._check_float('features_value', value)

        return features


class PutSignSession(object):

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = PutSignQuery()
        self._query.parse(data)

    def execute(self):
        logging.info('Put sign, lat = {}, long = {}'.format(self._query.latitude, self._query.longitude))

        info = SignInfo()
        info.user_id = self._query.user_id
        info.latitude = self._query.latitude
        info.longitude = self._query.longitude
        info.radius = self._query.radius
        info.time_to_live = self._query.time_to_live
        info.timestamp = self._query.timestamp
        info.is_private = self._query.is_private

        sign_id = self._sign_engine.put_sign(info, \
                                    self._query.features, \
                                    self._query.meta_blob, \
                                    self._query.object_blob, \
                                    self._query.image_blob, \
                                    self._query.preview_blob)

        self._users_engine.put_sign(info.user_id, sign_id)

        logging.info('Put succeed, sign_id = {}'.format(sign_id))
        return json.dumps({'success': True, 'sign_id': sign_id})


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = PutSignSession(global_context)
    s.parse_query('{"user_token": "123", "latitude": 1, "longitude": 4, "radius": 1, "features":[1,2,3], "is_private": false}')
    print s.execute()

