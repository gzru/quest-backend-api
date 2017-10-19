from query import Query
from users_engine import UsersEngine
from signs_engine import SignsEngine
import json
import logging


class GetUserViewsQuery(Query):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.limit = None
        self.cursor = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_token = self._get_required_str(tree, 'user_token')
        self.user_id = self._get_required_int64(tree, 'user_id')

        self.limit = self._get_optional_int64(tree, 'limit')
        if self.limit == None:
            self.limit = 100

        self.cursor = self._get_optional_str(tree, 'cursor')
        if self.cursor == None:
            self.cursor = ''


class GetUserViewsSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._signs_enging = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = GetUserViewsQuery()
        self._query.parse(data)

    def execute(self):
        page = self._users_engine.get_views(self._query.user_id, self._query.limit, self._query.cursor)

        views = list()
        for sign_id in page.data:
            view = {
                'sign_id': sign_id
            }
            views.append(view)

        result = {
            'data': views,
            'paging': { 'cursor': page.cursor_code, 'has_next': page.has_next }
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetUserViewsSession(global_context)
    s.parse_query('{ "user_token": "4553717682174717370", "user_id": 123, "limit": 2 }')
    print s.execute()

