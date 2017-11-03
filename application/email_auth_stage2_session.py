from query import Query
from profile_session_base import ProfileSessionBase
from auth_engine import AuthEngine
from users_engine import UsersEngine, UserInfo
import json
import logging


class EMailAuthStage2Query(Query):

    def __init__(self):
        self.auth_token = None
        self.auth_code = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.auth_token = self._get_required_str(tree, 'auth_token')
        self.auth_code = self._get_required_int64(tree, 'auth_code')

        logging.info('auth_token: {}'.format(self.auth_token))
        logging.info('auth_code: {}'.format(self.auth_code))


class EMailAuthStage2Session(object):

    def __init__(self, global_context):
        self._auth_engine = AuthEngine()
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = EMailAuthStage2Query()
        self._query.parse(data)

    def execute(self):
        email = self._auth_engine.auth_by_email_stage2(self._query.auth_code, self._query.auth_token)

        # Check user exists
        info = self._find_info(email)
        if info == None:
            info = UserInfo()
            info.name = ''
            info.email = email

            # Generate user id
            info.user_id = self._users_engine.gen_user_id(info)

            # Create profile
            self._users_engine.put_info(info)
            self._users_engine.put_external_link(info.user_id, email)

        result = {
            'success': True,
            'user_id': int(info.user_id),
            'user_token': str(info.user_id)
        }
        return json.dumps(result)

    def _find_info(self, email):
        user_id = self._users_engine.external_to_local_id(email)
        if user_id == None:
            return None
        try:
            return self._users_engine.get_info(user_id)
        except:
            return None


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = EMailAuthStage2Session(global_context)
    s.parse_query('{"auth_token": "eh7wj77ZzLSKciVaboAyHUah4HVCkP59LswgRICjBHrzXsPHSktYdtk/DDKPUJVbMHX43epZq/wlJWhZSIDNCQ==", "auth_code": 627}')
    print s.execute()

