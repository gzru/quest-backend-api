from sessions.session import POSTSession
from users_engine import UsersEngine


class Params(object):

    def __init__(self):
        self.user_token = None
        self.user_id = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.user_id = query.get_optional_int64('user_id', self.user_token.user_id)


class RemoveUserSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._access_rules = global_context.access_rules
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        # Check user credentials
        self._access_rules.check_can_edit_user_info(self._params.user_token, self._params.user_id)

        self._users_engine.remove_user(self._params.user_id)

        return {'success': True}

