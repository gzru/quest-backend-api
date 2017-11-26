from sessions.session import POSTSession


class Params(object):

    def __init__(self):
        self.user_id = None
        self.uuid = None

    def parse(self, query):
        self.user_id = query.get_required_int64('user_id')
        self.uuid = query.get_required_str('uuid')


class MakeMessagingTokenSession(POSTSession):

    def __init__(self, global_context):
        self._twilio_connector = global_context.twilio_connector
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        token = self._twilio_connector.make_messaging_token(self._params.user_id, self._params.uuid)
        result = {
            'success': True,
            'data': {
                'token': token
            }
        }
        return result

