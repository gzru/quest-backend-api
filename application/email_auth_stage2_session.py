from query import Query
from profile_session_base import UserInfo, ProfileSessionBase
from auth_engine import AuthEngine
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


class EMailAuthStage2Session(ProfileSessionBase):

    def __init__(self, global_context):
        super(EMailAuthStage2Session, self).__init__(global_context)
        self._auth_engine = AuthEngine()

    def parse_query(self, data):
        self._query = EMailAuthStage2Query()
        self._query.parse(data)

    def execute(self):
        email = self._auth_engine.auth_by_email_stage2(self._query.auth_code, self._query.auth_token)

        # Check user exists
        user_id = self._get_external_link(email)
        if user_id == None:
            info = UserInfo()
            info.name = ''
            info.email = email

            # Generate user id
            user_id = self._gen_user_id(info)

            # Create profile
            self._put_info(user_id, info)
            self._put_external_link(user_id, email)

        result = {
            'success': True,
            'user_id': int(user_id),
            'user_token': str(user_id)
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = EMailAuthStage2Session(global_context)
    s.parse_query('{"auth_token": "eh7wj77ZzLSKciVaboAyHUah4HVCkP59LswgRICjBHrzXsPHSktYdtk/DDKPUJVbMHX43epZq/wlJWhZSIDNCQ==", "auth_code": 6278}')
    print s.execute()

