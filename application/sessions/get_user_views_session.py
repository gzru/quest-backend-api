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
        self.user_id = query.get_optional_int64('user_id', self.user_token.user_id)
        self.limit = query.get_optional_int64('limit', 100)
        self.cursor = query.get_optional_str('cursor', u'')


class GetUserViewsSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._signs_enging = SignsEngine(global_context)
        self._access_rules = global_context.access_rules
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        # Check user credentials
        self._access_rules.check_can_view_private_info(self._params.user_token, self._params.user_id)

        page = self._users_engine.get_views(self._params.user_id, self._params.limit, self._params.cursor)

        views = list()
        for sign_id in page.data:
            view = {
                'sign_id': sign_id
            }
            views.append(view)

        result = {
            'data': views,
            'paging': { 'cursor': page.cursor_code, 'has_next': page.has_next }
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetUserViewsSession(global_context)
    s.parse_query('{ "user_token": "+sRTjHxdrxXKvHP22X1TaQ3CqGlpvwkWyDX0lpSvkfcma8iCxRmlkEpD/3/VjbyiWlCm9CtzrveOAuJkiEIh07MwzgNDSuBdbCYjrv/MFggfilGlg911Yd9mbEvw6mle", "user_id": 123, "limit": 2 }')
    print s.execute()

