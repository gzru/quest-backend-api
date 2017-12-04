from sessions.session import POSTSession
from signs_engine import SignsEngine


class Params(object):

    def __init__(self):
        self.user_token = None
        self.sign_id = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.sign_id = query.get_required_int64('sign_id')


class GrantSignAccessSession(POSTSession):

    def __init__(self, global_context):
        self._signs_engine = SignsEngine(global_context)
        self._access_rules = global_context.access_rules
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        # Check user credentials
        self._access_rules.check_can_edit_sign(self._params.user_token, sign_id=self._params.sign_id)

        self._signs_engine.grant_sign_access(self._params.user_id, self._params.sign_id)

        return {'success': True}


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GrantSignAccessSession(global_context)
    s.parse_query('{"user_id":123, "sign_id":345}')
    print s.execute()



