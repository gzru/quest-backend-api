from sessions.session import POSTSession
from users_engine import UsersEngine
from signs_engine import SignsEngine
import logging


class Params(object):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.limit = None
        self.cursor = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.user_id = query.get_required_int64('user_id')
        self.limit = query.get_optional_int64('limit', 100)
        self.cursor = query.get_optional_str('cursor', u'')


class GetUserLikesSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._signs_enging = SignsEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        page = self._users_engine.get_likes(self._params.user_id, self._params.limit, self._params.cursor)

        likes = list()
        for sign_id in page.data:
            like = {
                'sign_id': sign_id
            }
            likes.append(like)

        result = {
            'data': likes,
            'paging': { 'cursor': page.cursor_code, 'has_next': page.has_next }
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetUserLikesSession(global_context)
    s.parse_query('{ "user_token": "+sRTjHxdrxXKvHP22X1TaQ3CqGlpvwkWyDX0lpSvkfcma8iCxRmlkEpD/3/VjbyiWlCm9CtzrveOAuJkiEIh07MwzgNDSuBdbCYjrv/MFggfilGlg911Yd9mbEvw6mle", "user_id": 123, "limit": 2 }')
    print s.execute()

