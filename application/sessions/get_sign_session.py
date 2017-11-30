from sessions.session import POSTSession
from core.query import Query
from signs_engine import SignsEngine, SignInfo


class Params(object):

    def __init__(self):
        self.user_token = None
        self.sign_id = None
        self.properties = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.sign_id = query.get_required_int64('sign_id')
        self.properties = self._get_properties(query)

    def _get_properties(self, query):
        properties = query.get_optional_list('properties')
        if properties == None:
            return list()

        for value in properties:
            Query.check_str('properties_value', value)

        return properties


class GetSignSession(POSTSession):

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        info = self._sign_engine.get_info(self._params.sign_id)

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

        if 'meta_blob' in self._params.properties:
            result['meta_blob'] = self._sign_engine.get_meta(self._params.sign_id)

        if 'object_blob' in self._params.properties:
            result['object_blob'] = self._sign_engine.get_object(self._params.sign_id)

        if 'image_blob' in self._params.properties:
            result['image_blob'] = self._sign_engine.get_image(self._params.sign_id)

        if 'preview_blob' in self._params.properties:
            result['preview_blob'] = self._sign_engine.get_preview(self._params.sign_id)

        return result

