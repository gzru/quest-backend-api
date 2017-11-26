from sessions.session import POSTSession
from signs_engine import SignsEngine, SignInfo


class Params(object):

    def __init__(self):
        self.user_token = None
        self.sign_id = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.sign_id = query.get_required_int64('sign_id')


class GetSignStatsSession(POSTSession):

    def __init__(self, global_context):
        self._sign_engine = SignsEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        info = self._sign_engine.get_info(self._params.sign_id)
        if info == None:
            raise APILogicalError('Sign {} not found'.format(sign_id))

        result = {
            'success': True,
            'data': {
                'likes_count': info.likes_count,
                'views_count': info.views_count
            }
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetSignStatsSession(global_context)
    s.parse_query('{"user_token": "nlgKnIDLC0N5RkFRJHMTLwQwgzCxmwhUQspeKSeBChBipXEDChjbHqkrfzVBwnHV7kMn42rtICkDI7L+ttyHTR/3RqI7PN2G88KrxVX26+M=", "sign_id": 7803581012740187147}')
    print s.execute()

