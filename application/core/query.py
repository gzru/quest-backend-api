from error import APIQueryError
from core.access_token import AccessToken
import json
import base64
import numpy


class Query(object):

    USER_TOKEN_PARAM = 'user_token'

    def __init__(self, data = dict()):
        self._data = data

    def get_optional(self, name, default = None):
        value = self._data.get(name)
        if value == None:
            return default
        if isinstance(value, dict):
            return Query(value)
        return value

    def get_required(self, name):
        value = self.get_optional(name)
        if value == None:
            raise APIQueryError('Missed required query parameter "{}"'.format(name))
        return value

    def get_optional_int64(self, name, default = None):
        return Query.check_int64(name, self.get_optional(name, default))

    def get_required_int64(self, name):
        return Query.check_int64(name, self.get_required(name))

    def get_optional_float64(self, name, default = None):
        return Query.check_float(name, self.get_optional(name, default))

    def get_required_float64(self, name):
        return Query.check_float(name, self.get_required(name))

    def get_optional_str(self, name, default = None):
        return Query.check_str(name, self.get_optional(name, default))

    def get_required_str(self, name):
        return Query.check_str(name, self.get_required(name))

    def get_optional_bool(self, name, default = None):
        return Query.check_bool(name, self.get_optional(name, default))

    def get_required_bool(self, name):
        return Query.check_bool(name, self.get_required(name))

    def get_optional_list(self, name):
        return Query.check_list(name, self.get_optional(name))

    def get_required_list(self, name):
        return Query.check_list(name, self.get_required(name))

    ''' Extension for blobs '''
    def get_optional_blob(self, name):
        blob = self.get_optional(name)
        if blob == None:
            return None
        try:
            return base64.b64decode(blob)
        except Exception as ex:
            raise APIQueryError('"{}" have bad format'.format(name))

    def get_required_blob(self, name):
        blob = self.get_required(name)
        try:
            return base64.b64decode(blob)
        except Exception as ex:
            raise APIQueryError('"{}" have bad format'.format(name))

    ''' Extension for user token '''
    def get_user_token(self):
        token_string = self.get_required(Query.USER_TOKEN_PARAM)
        return AccessToken.from_encoded(token_string)

    @classmethod
    def from_json(cls, data):
        try:
            return cls(json.loads(data))
        except Exception as ex:
            raise APIQueryError('Can\'t parse json')

    @staticmethod
    def check_int64(name, value):
        if value == None:
            return None
        if isinstance(value, int) and numpy.int64(value) == value:
            return value
        raise APIQueryError('"{}" have bad format'.format(name))

    @staticmethod
    def check_float(name, value):
        if value == None:
            return None
        if (isinstance(value, float) or isinstance(value, int)) and \
            numpy.float64(value) == value:
            return value
        raise APIQueryError('"{}" have bad format'.format(name))

    @staticmethod
    def check_str(name, value):
        if value == None:
            return None
        if isinstance(value, unicode):
            return value
        raise APIQueryError('"{}" have bad format'.format(name))

    @staticmethod
    def check_bool(name, value):
        if value == None:
            return None
        if isinstance(value, bool):
            return value
        raise APIQueryError('"{}" have bad format'.format(name))

    @staticmethod
    def check_list(name, value):
        if value == None:
            return None
        if isinstance(value, list):
            return value
        raise APIQueryError('"{}" have bad format'.format(name))

