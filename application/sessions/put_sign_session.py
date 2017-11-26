from core.query import Query
from signs_engine import SignsEngine, SignInfo
from users_engine import UsersEngine
from sessions.session import POSTSession
import time
import logging


class Params(object):

    def __init__(self):
        self.user_token = None
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

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.latitude = query.get_required_float64('latitude')
        self.longitude = query.get_required_float64('longitude')
        self.radius = query.get_required_float64('radius')
        self.features = self._parse_features(query)
        # Blobs will be removed in a while
        self.meta_blob = query.get_optional_blob('meta_blob')
        self.object_blob = query.get_optional_blob('object_blob')
        self.image_blob = query.get_optional_blob('image_blob')
        self.preview_blob = query.get_optional_blob('preview_blob')
        self.time_to_live = query.get_optional_int64('time_to_live', 0)
        self.timestamp = query.get_optional_int64('timestamp', int(time.time()))
        self.is_private = query.get_optional_bool('is_private', True)

    def _parse_features(self, query):
        features = query.get_required_list('features')

        for value in features:
            Query.check_float('features_value', value)

        return features


class PutSignSession(POSTSession):

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)
        self._users_engine = UsersEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        logging.info('Put sign, lat = {}, long = {}'.format(self._params.latitude, self._params.longitude))

        info = SignInfo()
        info.user_id = self._params.user_token.user_id
        info.latitude = self._params.latitude
        info.longitude = self._params.longitude
        info.radius = self._params.radius
        info.time_to_live = self._params.time_to_live
        info.timestamp = self._params.timestamp
        info.is_private = self._params.is_private

        sign_id = self._sign_engine.put_sign(info, \
                                    self._params.features, \
                                    self._params.meta_blob, \
                                    self._params.object_blob, \
                                    self._params.image_blob, \
                                    self._params.preview_blob)

        self._users_engine.put_sign(info.user_id, sign_id)

        logging.info('Put succeed, sign_id = {}'.format(sign_id))
        return {'success': True, 'sign_id': sign_id}


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = PutSignSession(global_context)
    s.parse_query('{"user_token": "Kgb3MeBq1ztEVrcJuki4xPXGLuKCnwcGEAt35bHOoV1Vp8NJ2+4IlhnzH0S7fQDQovXSyKmRnbIRLZohj1SwGGo8BA3mgxhwm7zJwHkhiFk=", "latitude": 1, "longitude": 4, "radius": 1, "features":[1,2,3], "is_private": false}')
    print s.execute()

