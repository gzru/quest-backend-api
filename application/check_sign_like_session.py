from query import Query
from signs_engine import SignsEngine
import json


class CheckSignLikeQuery(Query):

    def __init__(self):
        self.user_id = None
        self.sign_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_required_int64(tree, 'user_id')
        self.sign_id = self._get_required_int64(tree, 'sign_id')


class CheckSignLikeSession(object):

    def __init__(self, global_context):
        self._signs_engine = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = CheckSignLikeQuery()
        self._query.parse(data)

    def execute(self):
        found = self._signs_engine.check_likes(self._query.user_id, [self._query.sign_id])

        return json.dumps({'success': True, 'found': found[0]})


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = CheckSignLikeSession(global_context)
    s.parse_query('{"user_id":12, "sign_id":345}')
    print s.execute()


