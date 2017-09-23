from query import Query, BadQuery
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
        self._aerospike_connector = global_context.aerospike_connector
        self._namespace = 'test'
        self._info_set = 'sign_info'
        self._meta_set = 'sign_meta'
        self._object_set = 'sign_object'
        self._image_set = 'sign_image'

    def parse_query(self, data):
        self._query = GetSignQuery()
        self._query.parse(data)

    def execute(self):
        result = self._get_info(self._query.sign_id)

        if 'meta_blob' in self._query.properties:
            result['meta_blob'] =  self._get_meta(self._query.sign_id)

        if 'object_blob' in self._query.properties:
            result['object_blob'] =  self._get_object(self._query.sign_id)

        if 'image_blob' in self._query.properties:
            result['image_blob'] =  self._get_image(self._query.sign_id)

        return json.dumps(result)

    def _get_info(self, sign_id):
        info_key = (self._namespace, self._info_set, str(sign_id))

        data = self._aerospike_connector.get_data(info_key)
        if not data:
            raise Exception('Sign {} not found'.format(sign_id))

        pdata = json.loads(str(data))

        result = dict()
        result['user_id'] = pdata['user_id']
        result['latitude'] = pdata.get('latitude')
        result['longitude'] = pdata.get('longitude')
        result['radius'] = pdata.get('radius')
        result['timestamp'] = pdata.get('timestamp')
        result['time_to_live'] = pdata.get('time_to_live')
        return result

    def _get_meta(self, sign_id):
        return self._get_blob((self._namespace, self._meta_set, str(sign_id)))

    def _get_object(self, sign_id):
        return self._get_blob((self._namespace, self._object_set, str(sign_id)))

    def _get_image(self, sign_id):
        return self._get_blob((self._namespace, self._image_set, str(sign_id)))

    def _get_blob(self, key):
        data = self._aerospike_connector.get_data(key)
        if not data:
            return None
        return base64.b64encode(data)

