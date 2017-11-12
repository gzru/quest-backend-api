from query import Query
from users_engine import UsersEngine
import json


class FindProfileQuery(Query):

    def __init__(self):
        self.facebook_user_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.facebook_user_id = self._get_optional_str(tree, 'facebook_user_id') 


class FindProfileSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = FindProfileQuery()
        self._query.parse(data)

    def execute(self):
        user_id = self._users_engine.external_to_local_id(self._query.facebook_user_id)

        return json.dumps({ 'success': True, 'user_id': user_id })


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = FindProfileSession(global_context)
    s.parse_query('{"user_token": "8618994807331250316", "facebook_user_id": "1675510119186846"}')
    print s.execute()

