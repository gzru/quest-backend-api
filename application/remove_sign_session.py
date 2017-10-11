from query import Query, BadQuery
from signs_engine import SignsEngine, SignInfo
import json


class RemoveSignQuery(Query):

    def __init__(self):
        self.user_token = None
        self.sign_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_token = self._get_optional_str(tree, 'user_token')
        self.sign_id = self._get_required_int64(tree, 'sign_id')


class RemoveSignSession:

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = RemoveSignQuery()
        self._query.parse(data)

    def execute(self):
        self._sign_engine.remove_sign(self._query.sign_id)

        return json.dumps({'success': True})

