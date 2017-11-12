from query import Query
from users_engine import UsersEngine
import json
import logging


class AddFriendsQuery(Query):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.friend_user_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_token = self._get_required_str(tree, 'user_token')
        self.user_id = self._get_optional_int64(tree, 'user_id')
        if self.user_id == None:
            self.user_id = int(self.user_token)
        self.friend_user_id = self._get_required_int64(tree, 'friend_user_id')


class AddFriendsSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = AddFriendsQuery()
        self._query.parse(data)

    def execute(self):
        self._users_engine.put_friend(self._query.user_id, self._query.friend_user_id)

        result = {
            'success': True,
            'data': None
        }
        return json.dumps(result)

