from query import Query
from signs_engine import SignsEngine
import json
import logging


class UpdateSignQuery(Query):

    def __init__(self):
        self.user_id = None
        self.sign_id = None
        self.is_private = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = int(self._get_required_str(tree, 'user_token'))
        self.sign_id = self._get_required_int64(tree, 'sign_id')
        self.is_private = self._get_optional_bool(tree, 'is_private')


class UpdateSignSession(object):

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = UpdateSignQuery()
        self._query.parse(data)

    def execute(self):
        if self._query.is_private != None:
            self._sign_engine.set_sign_privacy(self._query.sign_id, self._query.is_private)

        return json.dumps({'success': True})


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = UpdateSignSession(global_context)
    s.parse_query('{"user_token": "123", "sign_id": 2424028906593754110}')
    print s.execute()

