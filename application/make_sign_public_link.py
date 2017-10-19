from query import Query
import json
import requests
import logging


class MakeSignPublicLinkQuery(Query):

    def __init__(self):
        self.user_token = None
        self.sign_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_token = self._get_required_str(tree, 'user_token')
        self.sign_id = self._get_required_int64(tree, 'sign_id')


class MakeSignPublicLinkSession(object):

    def __init__(self, global_context):
        pass

    def parse_query(self, data):
        self._query = MakeSignPublicLinkQuery()
        self._query.parse(data)

    def execute(self):
        result = {
            'success': True,
            'link': 'https://quest.aiarlabs.com/app/pathtosign/id/' + str(self._query.sign_id)
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = MakeSignPublicLinkSession(global_context)
    s.parse_query('{ "user_token": "4553717682174717370", "sign_id": 123, "limit": 2 }')
    print s.execute()

