from sessions.session import POSTSession
from users_engine import UsersEngine


class Params(object):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.uuid = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.user_id = query.get_optional_int64('user_id', self.user_token.user_id)
        self.uuid = query.get_required_str('uuid')


class MakeMessagingTokenSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._twilio_connector = global_context.twilio_connector
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        # Check user exists
        info = self._users_engine.get_info(self._params.user_id)

        token = self._twilio_connector.make_messaging_token(self._params.user_id, self._params.uuid)
        result = {
            'success': True,
            'data': {
                'token': token
            }
        }
        return result

