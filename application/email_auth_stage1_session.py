from query import Query
from auth_engine import AuthEngine
import json
import logging


class EMailAuthStage1Query(Query):

    def __init__(self):
        self.email = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.email = self._get_required_str(tree, 'email')

        logging.info('email: {}'.format(self.email))


class EMailAuthStage1Session(object):

    def __init__(self, global_context):
        self._auth_engine = AuthEngine()

    def parse_query(self, data):
        self._query = EMailAuthStage1Query()
        self._query.parse(data)

    def execute(self):
        code_cipher = self._auth_engine.auth_by_email_stage1(self._query.email)

        result = {
            'success': True,
            'auth_token': code_cipher
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = EMailAuthStage1Session(global_context)
    s.parse_query('{"email": "gz_@bk.ru"}')
    print s.execute()

