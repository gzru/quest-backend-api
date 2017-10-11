from query import Query
from users_engine import UsersEngine
import json


class RemoveUserQuery(Query):

    def __init__(self):
        self.user_token = None
        self.user_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_token = self._get_optional_str(tree, 'user_token')
        self.user_id = self._get_required_int64(tree, 'user_id')


class RemoveUserSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = RemoveUserQuery()
        self._query.parse(data)

    def execute(self):
        self._users_engine.remove_user(self._query.user_id)

        return json.dumps({'success': True})

