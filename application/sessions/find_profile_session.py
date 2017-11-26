from sessions.session import POSTSession
from users_engine import UsersEngine


class Params(object):

    def __init__(self):
        self.facebook_user_id = None

    def parse(self, query):
        self.facebook_user_id = query.get_optional_str('facebook_user_id') 


class FindProfileSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        user_id = self._users_engine.external_to_local_id(self._params.facebook_user_id)

        return { 'success': True, 'user_id': user_id }


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = FindProfileSession(global_context)
    s.parse_query('{"user_token": "8618994807331250316", "facebook_user_id": "1675510119186846"}')
    print s.execute()

