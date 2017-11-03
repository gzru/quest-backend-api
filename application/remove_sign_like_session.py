from query import Query
from signs_engine import SignsEngine
from users_engine import UsersEngine
import json


class RemoveSignLikeQuery(Query):

    def __init__(self):
        self.user_id = None
        self.sign_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_required_int64(tree, 'user_id')
        self.sign_id = self._get_required_int64(tree, 'sign_id')


class RemoveSignLikeSession(object):

    def __init__(self, global_context):
        self._signs_engine = SignsEngine(global_context)
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = RemoveSignLikeQuery()
        self._query.parse(data)

    def execute(self):
        self._signs_engine.remove_sign_like(self._query.user_id, self._query.sign_id)
        self._users_engine.remove_like(self._query.user_id, self._query.sign_id)

        return json.dumps({'success': True})


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = RemoveSignLikeSession(global_context)
    s.parse_query('{"user_id":123, "sign_id":345}')
    print s.execute()



