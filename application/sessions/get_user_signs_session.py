from sessions.session import POSTSession
from users_engine import UsersEngine
from signs_engine import SignsEngine
from error import APIQueryError
import logging


PRIVACY_PUBLIC = 1
PRIVACY_PRIVATE = 2
PRIVACY_ALL = 3

class Params(object):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.limit = None
        self.cursor = None
        self.privacy = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.user_id = query.get_optional_int64('user_id', self.user_token.user_id)
        self.limit = query.get_optional_int64('limit', 100)
        self.cursor = query.get_optional_str('cursor', u'')

        privacy_str = query.get_optional_str('privacy')
        if privacy_str == 'PUBLIC':
            self.privacy = PRIVACY_PUBLIC
        elif privacy_str == 'PRIVATE':
            self.privacy = PRIVACY_PRIVATE
        elif privacy_str == 'ALL':
            self.privacy = PRIVACY_ALL
        elif privacy_str == None:
            # default
            self.privacy = PRIVACY_PUBLIC
        else:
            raise APIQueryError('Unknown privacy enum value')


class GetUserSignsSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._signs_enging = SignsEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        page = self._users_engine.get_signs(self._params.user_id, self._params.limit, self._params.cursor)

        signs = list()
        # Retrieve aux data
        is_private_list = self._signs_enging.check_privacy_many(page.data)
        for i in range(len(page.data)):
            if is_private_list[i] == None:
                logging.warning('Check privacy for unknown sign {}'.format(page.data[i]))
                continue
            if (is_private_list[i] and self._params.privacy == PRIVACY_PUBLIC) \
                or (not is_private_list[i] and self._params.privacy == PRIVACY_PRIVATE):
                continue

            sign = {
                'sign_id': page.data[i],
                'is_private': is_private_list[i]
            }
            signs.append(sign)

        result = {
            'data': signs,
            'paging': { 'cursor': page.cursor_code, 'has_next': page.has_next }
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetUserSignsSession(global_context)
    s.parse_query('{ "user_token": "4msU7R3qOnQ6o+QDS76PUtgcrnbVSh9lAkxM4peLfJQhLn6XiIWp3ZVN71W/YGmytfP7KT/zNUb3fXydyvdtn7lNKLYTXuj8Eoa6mifuNrCbg8ItRllEhImy/cSFWUCa", "user_id": 4553717682174717370, "limit": 24, "privacy": "PUBLIC" }')
    print s.execute()

