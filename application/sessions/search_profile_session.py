from sessions.session import POSTSession
from users_engine import UsersEngine


class Params(object):

    def __init__(self):
        self.keywords = None

    def parse(self, query):
        self.keywords = query.get_required_str('keywords')


class SearchProfileSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        found = self._users_engine.search(self._params.keywords)

        result = {
            'success': True,
            'data': found
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = SearchProfileSession(global_context)
    s.parse_query('{"keywords": "asd"}')
    print s.execute()

