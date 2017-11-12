from global_context import GlobalContext
from error import APILogicalError
import base64
import json
import hashlib


class DataPage(object):

    def __init__(self):
        self.data = list()
        self.cursor_code = ''
        self.has_next = False


class Cursor(object):

    def __init__(self, bucket_id = 0, offset = 0):
        self.bucket_id = bucket_id
        self.offset = offset

    def encode(self):
        serialized = json.dumps({ 'b': self.bucket_id, 'o': self.offset })
        return base64.b64encode(serialized)

    def decode(self, code):
        if code == None or code == '':
            self.bucket_id = 0
            self.offset = 0
            return

        try:
            serialized = base64.b64decode(code)
            data = json.loads(serialized)
            self.bucket_id = data['b']
            self.offset = data['o']
        except Exception as ex:
            # log
            raise Exception('Bad cursor')


class UserInfo(object):

    def __init__(self):
        self.user_id = None
        self.facebook_user_id = None
        self.facebook_access_token = None
        self.name = None
        self.username = None
        self.email = None

    def encode(self):
        # blob
        data = {
            'user_id': self.user_id,
            'facebook_user_id': self.facebook_user_id,
            'facebook_access_token': self.facebook_access_token,
            'name': self.name,
            'username': self.username,
            'email': self.email
        }
        # a record itself
        record = {
            'data': json.dumps(data),
            'user_id': self.user_id,
            'fb_user_id': self.facebook_user_id,
            'fb_token': self.facebook_access_token,
            'name': self.name,
            'username': self.username,
            'email': self.email
        }
        return record

    def decode(self, record):
        # TODO: remove data json
        data = json.loads(record['data'])
        def _get(prop):
            return record.get(prop, data.get(prop))

        self.user_id = _get('user_id')
        self.facebook_user_id = record.get('fb_user_id', data.get('facebook_user_id'))
        self.facebook_access_token = record.get('fb_token', data.get('facebook_access_token'))
        self.name = _get('name')
        self.username = _get('username')
        self.email = _get('email')


class UsersRelation(object):

    def __init__(self):
        self.is_friends = None
        self.twilio_channel = None

    def encode(self):
        record = {
            'friends': int(self.is_friends),
            'twilio_channel': self.twilio_channel
        }
        return record

    def decode(self, record):
        self.is_friends = bool(record.get('friends', False))
        self.twilio_channel = record.get('twilio_channel')


# TODO: Buckets
class UsersEngine(object):

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._twilio_connector = global_context.twilio_connector
        self._s3connector = global_context.s3connector
        self._namespace = 'test'
        self._info_set = 'user_info'
        self._meta_set = 'user_meta'
        self._picture_set = 'user_picture'
        self._friends_set = 'user_friends'
        self._signs_set = 'user_signs'
        self._likes_set = 'user_likes'
        self._views_set = 'user_views'
        self._external_to_local_set = 'user_external_to_local'
        self._relations_set = 'relations_user_user'

    def gen_user_id(self, info):
        hasher = hashlib.md5()
        def _update(value):
            if value:
                hasher.update(str(value))
        _update(info.facebook_user_id)
        _update(info.email)
        return int(hasher.hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF

    def put_info(self, info):
        info_key = (self._namespace, self._info_set, str(info.user_id))
        self._aerospike_connector.put_bins(info_key, info.encode())

    def get_info(self, user_id):
        info_key = (self._namespace, self._info_set, str(user_id))

        record = self._aerospike_connector.get_bins(info_key)
        if record == None:
            raise APILogicalError('User {} not found'.format(user_id))

        try:
            info = UserInfo()
            info.decode(record)
        except Exception as ex:
            logging.error('Bad info record, {}'.format(ex))
            raise APILogicalError('Bad info record, {}'.format(ex))
        return info

    def put_picture(self, user_id, picture):
        pic_key = (self._namespace, self._picture_set, str(user_id))
        self._aerospike_connector.put_data(pic_key, picture)
        self._s3connector.put_data_object('content', 'profile/{}/picture'.format(user_id), picture)

    def get_picture(self, user_id):
        pic_key = (self._namespace, self._picture_set, str(user_id))
        data = self._aerospike_connector.get_data(pic_key)
        if data == None:
            return None
        return base64.b64encode(data)

    def put_meta(self, user_id, meta):
        meta_key = (self._namespace, self._meta_set, str(user_id))
        self._aerospike_connector.put_data(meta_key, meta)

    def get_meta(self, user_id):
        meta_key = (self._namespace, self._meta_set, str(user_id))
        data = self._aerospike_connector.get_data(meta_key)
        if not data:
            return None
        return base64.b64encode(data)

    def remove_user(self, user_id):
        self._aerospike_connector.remove((self._namespace, self._info_set, str(user_id)))
        self._aerospike_connector.remove((self._namespace, self._friends_set, str(user_id)))
        self._aerospike_connector.remove((self._namespace, self._signs_set, str(user_id)))

    def put_friend(self, user_id, friend_user_id):
        relation_key = self._make_relation_key(user_id, friend_user_id)
        if self._aerospike_connector.check_exists((self._namespace, self._relations_set, relation_key)):
            raise Exception('Users already friends')

        relation = UsersRelation()
        relation.is_friends = True
        relation.twilio_channel = self._twilio_connector.create_channel(user_id, friend_user_id)

        self._put_item_to_list((self._namespace, self._friends_set, str(user_id)), int(friend_user_id))
        self._put_item_to_list((self._namespace, self._friends_set, str(friend_user_id)), int(user_id))
        self._aerospike_connector.put_bins((self._namespace, self._relations_set, relation_key), relation.encode())

    def get_friends(self, user_id, limit, cursor_code):
        return self._get_items_from_list((self._namespace, self._friends_set, str(user_id)), limit, cursor_code)

    def put_external_link(self, user_id, external_id):
        if external_id == None:
            return
        link_key = (self._namespace, self._external_to_local_set, external_id)
        record = {
            'user_id': str(user_id)
        }
        self._aerospike_connector.put_bins(link_key, record)

    def external_to_local_id(self, external_id):
        if external_id == None:
            return None
        link_key = (self._namespace, self._external_to_local_set, external_id)
        record = self._aerospike_connector.get_bins(link_key)
        if record == None:
            return None
        user_id = record.get('user_id')
        if user_id == None:
            raise APILogicalError('External link record({}) has no user_id bin'.format(external_id))
        return int(user_id)

    def external_to_local_id_many(self, external_ids):
        keys = list()
        for ext_id in external_ids:
            keys.append((self._namespace, self._external_to_local_set, str(ext_id)))

        res = self._aerospike_connector.get_bins_many(keys)

        local_ids = list()
        for entry in res:
            if entry == None:
                local_ids.append(None)
            else:
                user_id = entry.get('user_id')
                if user_id == None:
                    local_ids.append(None)
                else:
                    local_ids.append(int(user_id))
        return local_ids

    def get_relations_many(self, user_id, friends_ids):
        keys = list()
        for friend_user_id in friends_ids:
            keys.append((self._namespace, self._relations_set, self._make_relation_key(user_id, friend_user_id)))

        res = self._aerospike_connector.get_bins_many(keys)

        results = list()
        for entry in res:
            if entry == None:
                results.append(None)
            else:
                relation = UsersRelation()
                relation.decode(entry)
                results.append(relation)
        return results

    def check_friends_many(self, user_id, friends_ids):
        res = self.get_relations_many(user_id, friends_ids)

        check_results = list()
        for relation in res:
            if relation != None and relation.is_friends == True:
                check_results.append(True)
            else:
                check_results.append(False)
        return check_results

    def search(self, keywords):
        res = self._aerospike_connector.scan(self._namespace, self._info_set, ['data'])

        result = list()
        for entry in res:
            data = json.loads(entry.get('data'))
            user_id = data.get('user_id')
            name = data.get('name')
            if user_id == None or name == None:
                continue
            profile = {
                'user_id': user_id,
                'name': name
            }
            result.append(profile)
        return result

    def put_sign(self, user_id, sign_id):
        self._put_item_to_list((self._namespace, self._signs_set, str(user_id)), int(sign_id))

    def get_signs(self, user_id, limit, cursor_code):
        return self._get_items_from_list((self._namespace, self._signs_set, str(user_id)), limit, cursor_code)

    def put_like(self, user_id, sign_id):
        self._put_item_to_list((self._namespace, self._likes_set, str(user_id)), int(sign_id))

    def get_likes(self, user_id, limit, cursor_code):
        return self._get_items_from_list((self._namespace, self._likes_set, str(user_id)), limit, cursor_code)

    def remove_like(self, user_id, sign_id):
        self._remove_item_from_list((self._namespace, self._likes_set, str(user_id)), int(sign_id))

    def put_view(self, user_id, sign_id):
        self._put_item_to_list((self._namespace, self._views_set, str(user_id)), int(sign_id))

    def get_views(self, user_id, limit, cursor_code):
        return self._get_items_from_list((self._namespace, self._views_set, str(user_id)), limit, cursor_code)

    def remove_view(self, user_id, sign_id):
        self._remove_item_from_list((self._namespace, self._views_set, str(user_id)), int(sign_id))

    def _put_item_to_list(self, user_key, item):
        self._aerospike_connector.list_append(user_key, 'data', item)

    def _get_items_from_list(self, user_key, limit, cursor_code):
        result = DataPage()

        cursor = Cursor()
        cursor.decode(cursor_code)

        items_count = self._aerospike_connector.list_size(user_key, 'data')
        query_count = limit
        if cursor.offset + query_count >= items_count:
            query_count = items_count - cursor.offset
            result.has_next = False
        else:
            result.has_next = True

        if query_count <= 0:
            return result

        result.data = self._aerospike_connector.list_get_range(user_key, 'data', cursor.offset, query_count)
        if result.data == None:
            raise APILogicalError('Can\'t get items range from database')

        next_cursor = Cursor(0, cursor.offset + query_count)
        result.cursor_code = next_cursor.encode()

        return result

    def _remove_item_from_list(self, user_key, item):
        items_count = self._aerospike_connector.list_size(user_key, 'data')
        if items_count == 0:
            raise APILogicalError('Can\'t remove item from empty list')
        items = self._aerospike_connector.list_get_range(user_key, 'data', 0, items_count)
        for i in range(len(items) - 1, -1, -1):
            if items[i] == item:
                self._aerospike_connector.list_remove(user_key, 'data', i)

    def _make_relation_key(self, user_id1, user_id2):
        if user_id1 < user_id2:
            return '{}:{}'.format(user_id1, user_id2)
        else:
            return '{}:{}'.format(user_id2, user_id1)


if __name__ == "__main__":
    global_context = GlobalContext()
    global_context.initialize()

    ue = UsersEngine(global_context)
    #print ue.get_info(8147102016349374469).encode()
    #print ue.search("ivan")

    #ue.put_like(123, 346)
    #ue.get_friends(123, 3, '')
    ue.remove_like(123, 346)
    print ue.get_likes(123, 10, '').data

    #ue.put_sign(123, 5136828351633214532)
    #page = ue.get_signs(123, 1, '')
    #print page.data
    #print page.cursor_code
    #print page.has_next

