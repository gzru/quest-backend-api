from sessions.session import POSTSession
from auth_engine import AuthEngine
from users_engine import UsersEngine, UserInfo
import logging


class Params(object):

    def __init__(self):
        self.auth_token = None
        self.auth_code = None

    def parse(self, query):
        self.auth_token = query.get_required_str('auth_token')
        self.auth_code = query.get_required_int64('auth_code')


class EMailAuthStage2Session(POSTSession):

    def __init__(self, global_context):
        self._auth_engine = AuthEngine()
        self._users_engine = UsersEngine(global_context)
        self._avatar_generator = global_context.avatar_generator
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        email = self._auth_engine.auth_by_email_stage2(self._params.auth_code, self._params.auth_token)

        # Check user exists
        user_id = self._find_user(email)
        if user_id == None:
            user_id = self._create_new_user(email)

        result = {
            'success': True,
            'user_id': int(user_id),
            'user_token': str(user_id)
        }
        return result

    def _find_user(self, email):
        user_id = self._users_engine.external_to_local_id(email)
        if user_id == None:
            return None
        try:
            info = self._users_engine.get_info(user_id)
            return info.user_id
        except:
            return None

    # TODO: move into users_engine
    def _create_new_user(self, email):
        info = UserInfo()
        info.name = ''
        info.email = email

        # Generate user id
        info.user_id = self._users_engine.gen_user_id(info)

        logging.info('create new profile, email = {}, user_id = {}'.format(email, info.user_id))

        # Create profile
        self._users_engine.put_info(info)
        self._users_engine.put_external_link(info.user_id, email)

        picture = self._avatar_generator.generate(email)
        self._users_engine.put_picture(info.user_id, picture)

        return info.user_id


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = EMailAuthStage2Session(global_context)
    s.parse_query('{"auth_token": "eh7wj77ZzLSKciVaboAyHUah4HVCkP59LswgRICjBHrzXsPHSktYdtk/DDKPUJVbMHX43epZq/wlJWhZSIDNCQ==", "auth_code": 6278}')
    print s.execute()

