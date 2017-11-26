from sessions.session import POSTSession
from error import APILogicalError
from users_engine import UsersEngine


class Params(object):

    def __init__(self):
        self.user_token = None
        self.name = None
        self.username = None
        self.email = None
        self.meta_blob = None
        self.picture_blob = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.name = query.get_optional_str('name')
        self.username = query.get_optional_str('username')
        self.email = query.get_optional_str('email')
        self.meta_blob = query.get_optional_blob('meta_blob')
        self.picture_blob = query.get_optional_blob('picture_blob')


class UpdateProfileSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        user_id = self._params.user_token.user_id
        user_info = self._users_engine.get_info(user_id)
        if user_info == None:
            raise APILogicalError('User not found')

        info_updated = False
        if self._params.name != None: user_info.name = self._params.name; info_updated = True
        if self._params.username != None: user_info.username = self._params.username; info_updated = True
        if self._params.email != None: user_info.email = self._params.email; info_updated = True
        if info_updated:
            self._users_engine.put_info(user_info)

        if self._params.meta_blob:
            self._users_engine.put_meta(user_id, self._params.meta_blob)

        if self._params.picture_blob != None:
            self._users_engine.put_picture(user_id, self._params.picture_blob)

        result = {
            'success': True,
            'user_token': self._params.user_token.encode()
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = UpdateProfileSession(global_context)
    s.parse_query('{"user_token": "IzPN8JgK5ohnv1JvnUZz1KKnXDetU72BlcY2ncwsER/a7Mzb1RZATNzx4s/f/A6f6FRpt2I7g605qAcGo1rsjTh+1GNzmmzUxpBjp4Ai/XNFWlyHrfB8DpQftgDkcMV4", "name": "Ivan Ivanov"}')
    print s.execute()

