from sessions.session import POSTSession
from signs_engine import SignsEngine
import logging


class Params(object):

    def __init__(self):
        self.user_token = None
        self.sign_id = None
        self.is_private = None


class UpdateSignSession(POSTSession):

    def __init__(self, global_context):
        self._params = Params()
        self._sign_engine = SignsEngine(global_context)
        self._access_rules = global_context.access_rules

    def _init_session_params(self, query):
        self._params.user_token = query.get_user_token()
        self._params.sign_id = query.get_required_int64('sign_id')
        self._params.is_private = query.get_optional_bool('is_private')

    def _run_session(self):
        info = self._sign_engine.get_info(self._params.sign_id)

        # Check user credentials
        self._access_rules.check_can_edit_sign(self._params.user_token, sign_info=info)

        if self._params.is_private != None:
            self._sign_engine.set_sign_privacy(self._params.sign_id, self._params.is_private)

        return {'success': True}


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = UpdateSignSession(global_context)
    s.parse_query('{"user_token": "Gn0qsNoPY2BJGhfVYpKaVilLcClAMhbkOUocm6wTCEYfrL2sdzOs3FDVPOKk/paM9rsRALIS5VnnNxPGgcq33gBrcuy5cvHsSddQY8SqGNJ5lBDPZC4nT4imTibF+Uz/", "sign_id": 6553327435719305655}')
    print s.execute()

