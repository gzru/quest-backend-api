from sessions.session import POSTSession
from users_engine import UsersEngine


class Params(object):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.friend_user_id = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.user_id = self.user_token.user_id
        self.friend_user_id = query.get_required_int64('friend_user_id')


class AddFriendsSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        self._users_engine.put_friend(self._params.user_id, self._params.friend_user_id)

        result = {
            'success': True,
            'data': None
        }
        return result

