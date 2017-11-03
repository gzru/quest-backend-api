from query import Query, BadQuery
from signs_engine import SignsEngine, SignInfo
import json


class GetSignStatsQuery(Query):

    def __init__(self):
        self.user_token = None
        self.sign_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_token = self._get_required_str(tree, 'user_token')
        self.sign_id = self._get_required_int64(tree, 'sign_id')


class GetSignStatsSession(object):

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = GetSignStatsQuery()
        self._query.parse(data)

    def execute(self):
        info = self._sign_engine.get_info(self._query.sign_id)
        if info == None:
            raise APILogicalError('Sign {} not found'.format(sign_id))

        result = {
            'success': True,
            'data': {
                'likes_count': info.likes_count,
                'views_count': info.views_count
            }
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetSignStatsSession(global_context)
    s.parse_query('{"user_token": "123", "sign_id": 7803581012740187147}')
    print s.execute()

