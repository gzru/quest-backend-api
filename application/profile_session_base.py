from query import Query, BadQuery
from users_engine import UsersEngine
import json
import hashlib
import base64


class UserInfo(object):

    def __init__(self):
        self.user_id = None
        self.email = None
        self.facebook_user_id = None
        self.facebook_access_token = None
        self.name = None


class ProfileQueryBase(Query):

    def _parse_friends(self, tree):
        friends = tree.get('friends')
        if not friends:
            return None

        if not isinstance(friends, list):
            raise BadQuery('"friends" have bad format')

        result = list()
        for entry in friends:
            friend = dict()
            friend['user_id'] = self._get_optional_int64(entry, 'user_id')
            friend['facebook_user_id'] = self._get_optional_str(entry, 'facebook_user_id')
            friend['name'] = self._get_optional_str(entry, 'name')
            result.append(friend)
        return result


class ProfileSessionBase(object):

    def __init__(self, global_context):
        self._engine = UsersEngine(global_context)
        self._aerospike_connector = global_context.aerospike_connector
        self._namespace = 'test'
        self._info_set = 'user_info'
        self._meta_set = 'user_meta'
        self._friends_set = 'user_friends'
        self._external_to_local_set = 'user_external_to_local'

    def _gen_user_id(self, query):
        hasher = hashlib.md5()
        def _update(value):
            if value:
                hasher.update(str(value))
        _update(query.facebook_user_id)
        _update(query.email)
        return int(hasher.hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF

    def _check_exists(self, user_id):
        info_key = (self._namespace, self._info_set, str(user_id))
        return self._aerospike_connector.check_exists(info_key)

    def _put_info(self, user_id, query):
        info_key = (self._namespace, self._info_set, str(user_id))
        info = {
            'user_id': user_id,
            'facebook_user_id': query.facebook_user_id,
            'facebook_access_token': query.facebook_access_token,
            'name': query.name,
            'email': query.email
        }
        bins = {
            'data': json.dumps(info)
        }
        self._aerospike_connector.put_bins(info_key, bins)

    def _get_info(self, user_id):
        info_key = (self._namespace, self._info_set, str(user_id))

        bins = self._aerospike_connector.get_bins(info_key)
        if not bins:
            raise Exception('User {} not found'.format(user_id))

        data = bins.get('data')
        if not data:
            raise Exception('User info has no data bin, user_id = {}'.format(user_id))

        parsed = json.loads(data)

        result = dict()
        result['user_id'] = parsed['user_id']
        result['facebook_user_id'] = parsed.get('facebook_user_id')
        result['facebook_access_token'] = parsed.get('facebook_access_token')
        result['name'] = parsed.get('name')
        result['email'] = parsed.get('email')
        return result

    def _put_meta(self, user_id, query):
        if not query.meta_blob:
            return
        meta_key = (self._namespace, self._meta_set, str(user_id))
        if not self._aerospike_connector.put_data(meta_key, query.meta_blob):
            raise Exception('Can\'t save meta blob')

    def _get_meta(self, user_id):
        data = self._aerospike_connector.get_data((self._namespace, self._meta_set, str(user_id)))
        if not data:
            return None
        return base64.b64encode(data)

    def _put_friends(self, user_id, query):
        if not query.friends:
            return
        friends_key = (self._namespace, self._friends_set, str(user_id))
        if not self._aerospike_connector.put_data(friends_key, json.dumps(query.friends)):
            raise Exception('Can\'t save friends')

    def _get_friends(self, user_id):
        data = self._aerospike_connector.get_data((self._namespace, self._friends_set, str(user_id)))
        if not data:
            return None
        return json.loads(str(data))

    def _put_external_link(self, user_id, facebook_user_id):
        if not facebook_user_id:
            return
        link_key = (self._namespace, self._external_to_local_set, facebook_user_id)
        bins = {
            'user_id': str(user_id)
        }
        self._aerospike_connector.put_bins(link_key, bins)

    def _get_external_link(self, facebook_user_id):
        if not facebook_user_id:
            return None
        link_key = (self._namespace, self._external_to_local_set, facebook_user_id)
        bins = self._aerospike_connector.get_bins(link_key)
        if not bins:
            return None
        user_id = bins.get('user_id')
        if not user_id:
            raise Exception('External link record has no user_id bin')
        return user_id

