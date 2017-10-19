from query import Query, BadQuery
from signs_engine import SignsEngine, SignInfo
import json
import base64


class GetSignQuery(Query):

    def __init__(self):
        self.user_id = None
        self.sign_id = None
        self.properties = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_optional_int64(tree, 'user_id')
        self.sign_id = self._get_required_int64(tree, 'sign_id')
        self.properties = self._get_properties(tree)

    def _get_properties(self, tree):
        properties = tree.get('properties')
        if not properties:
            return list()

        if not isinstance(properties, list):
            raise BadQuery('"properties" have bad format')

        for value in properties:
            self._check_str('properties_value', value)

        return properties


class GetSignSession:

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = GetSignQuery()
        self._query.parse(data)

    def execute(self):
        info = self._sign_engine.get_info(self._query.sign_id)

        result = {
            'sign_id': info.sign_id,
            'user_id': info.user_id,
            'latitude': info.latitude,
            'longitude': info.longitude,
            'radius': info.radius,
            'timestamp': info.timestamp,
            'time_to_live': info.time_to_live,
            'is_private': info.is_private
        }

        if 'meta_blob' in self._query.properties:
            result['meta_blob'] = self._sign_engine.get_meta(self._query.sign_id)

        if 'object_blob' in self._query.properties:
            result['object_blob'] = self._sign_engine.get_object(self._query.sign_id)

        if 'image_blob' in self._query.properties:
            result['image_blob'] = self._sign_engine.get_image(self._query.sign_id)

        if 'preview_blob' in self._query.properties:
            result['preview_blob'] = self._sign_engine.get_preview(self._query.sign_id)

        return json.dumps(result)

