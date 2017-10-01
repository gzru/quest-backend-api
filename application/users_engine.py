from global_context import GlobalContext
import base64
import json


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


# TODO: Buckets
class UsersEngine(object):

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._namespace = 'test'
        self._info_set = 'user_info'
        self._friends_set = 'user_friends'
        self._external_to_local_set = 'user_external_to_local'
        self._relations_set = 'relations_user_user'
        self._friends_bin = 'data'

    def put_friend(self, user_id, friend_user_id):
        relation_key = '{}:{}'.format(user_id, friend_user_id)
        if self._aerospike_connector.check_exists((self._namespace, self._relations_set, relation_key)):
            raise Exception('Users already friends')

        self._aerospike_connector.list_append((self._namespace, self._friends_set, str(user_id)), self._friends_bin, int(friend_user_id))
        self._aerospike_connector.put_bins((self._namespace, self._relations_set, relation_key), {'friends': 1})

    def get_friends(self, user_id, limit, cursor_code):
        user_key = (self._namespace, self._friends_set, str(user_id))

        result = DataPage()

        cursor = Cursor()
        cursor.decode(cursor_code)

        friends_count = self._aerospike_connector.list_size(user_key, self._friends_bin)
        query_count = limit
        if cursor.offset + query_count >= friends_count:
            query_count = friends_count - cursor.offset
            result.has_next = False
        else:
            result.has_next = True

        if query_count <= 0:
            return result

        result.data = self._aerospike_connector.list_get_range(user_key, self._friends_bin, cursor.offset, query_count)
        if result.data == None:
            raise Exception('Can\'t get friends range from database')

        next_cursor = Cursor(0, cursor.offset + query_count)
        result.cursor_code = next_cursor.encode()

        return result

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

    def check_friends_many(self, user_id, friends_ids):
        keys = list()
        for friend_user_id in friends_ids:
            keys.append((self._namespace, self._relations_set, '{}:{}'.format(user_id, friend_user_id)))

        res = self._aerospike_connector.get_bins_many(keys)

        check_results = list()
        for entry in res:
            if entry == None:
                check_results.append(False)
            else:
                check_results.append(True)
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


if __name__ == "__main__":
    global_context = GlobalContext()
    global_context.initialize()

    ue = UsersEngine(global_context)
    print ue.search("ivan")

    #ue.put_friend(123, 346)
    #ue.get_friends(123, 3, '')

