from sessions.session import POSTSession
from users_engine import UsersEngine
import logging


class Params(object):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.limit = None
        self.cursor = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.user_id = query.get_optional_int64('user_id', self.user_token.user_id)
        self.limit = query.get_optional_int64('limit', 100)
        self.cursor = query.get_optional_str('cursor', u'')


class GetFriendsSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._access_rules = global_context.access_rules
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        # Check user credentials
        self._access_rules.check_can_view_private_info(self._params.user_token, self._params.user_id)

        page = self._users_engine.get_friends(self._params.user_id, self._params.limit, self._params.cursor)
        relations = self._users_engine.get_relations_many(self._params.user_id, page.data)

        friends = list()
        # Retrieve aux data
        for i, user_id in enumerate(page.data):
            if relations[i] == None or not relations[i].is_friends:
                continue

            # FIXME use group request
            info = None
            try:
                info = self._users_engine.get_info(user_id)
            except:
                pass
            if info == None:
                logging.warning('Can\'t find user profile, id = {}'.format(user_id))
                continue

            profile = {
                'user_id': info.user_id,
                'facebook_user_id': info.facebook_user_id,
                'name': info.name,
                'username': info.username,
                'email': info.email,
                'twilio_channel': relations[i].twilio_channel
            }
            friends.append(profile)

        result = {
            'success': True,
            'data': friends,
            'paging': { 'cursor': page.cursor_code, 'has_next': page.has_next }
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetFriendsSession(global_context)
    s.parse_query('{"user_token": "vSwd8QUz97grmevqFms77ksVwFVRAgK+D7XiilnbmsrY4sjTM5/qUTMa2nlPqcQKp8Fm1y8PgXYHppaHdQga+xV2dRmnwLMx02dC6XN7SCz8PLj9kcW2iAnRMBwBbk6o", "user_id": 8618994807331250316, "properties": ["friends"]}')
    print s.execute()

