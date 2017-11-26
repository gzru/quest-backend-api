from sessions.session import POSTSession
from signs_engine import SignsEngine
from users_engine import UsersEngine


class Params(object):

    def __init__(self):
        self.user_id = None
        self.sign_id = None

    def parse(self, query):
        self.user_id = query.get_required_int64('user_id')
        self.sign_id = query.get_required_int64('sign_id')


class AddSignLikeSession(POSTSession):

    def __init__(self, global_context):
        self._signs_engine = SignsEngine(global_context)
        self._users_engine = UsersEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        self._signs_engine.add_sign_like(self._params.user_id, self._params.sign_id)
        self._users_engine.put_like(self._params.user_id, self._params.sign_id)

        return {'success': True}


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = AddSignLikeSession(global_context)
    s.parse_query('{"user_id":123, "sign_id":345}')
    print s.execute()



