from sessions.session import POSTSession
from auth_engine import AuthEngine
import logging


class Params(object):

    def __init__(self):
        self.email = None

    def parse(self, query):
        self.email = query.get_required_str('email')


class EMailAuthStage1Session(POSTSession):

    def __init__(self, global_context):
        self._auth_engine = AuthEngine()
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        code_cipher = self._auth_engine.auth_by_email_stage1(self._params.email)

        result = {
            'success': True,
            'auth_token': code_cipher
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = EMailAuthStage1Session(global_context)
    s.parse_query('{"email": "kitty0x29a@gmail.com"}')
    print s.execute()

