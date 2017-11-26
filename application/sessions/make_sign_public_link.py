from sessions.session import POSTSession
import logging


class Params(object):

    def __init__(self):
        self.user_token = None
        self.sign_id = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.sign_id = query.get_required_int64('sign_id')


class MakeSignPublicLinkSession(POSTSession):

    def __init__(self, global_context):
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        result = {
            'success': True,
            'link': 'https://quest.aiarlabs.com/app/pathtosign/id/' + str(self._params.sign_id)
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = MakeSignPublicLinkSession(global_context)
    s.parse_query('{ "user_token": "zaqru5T2VewRZxZp8rjDtXpSEzR7mpMKwGcG1JEXnMoReyGY+OpYfbCFzJdXkgtoEObuGiYHNGmZP18w6Cjw4lCHKAw3ec8cApeeOn73Pq77+J23RMY7cBIyM7IlCuwA", "sign_id": 123, "limit": 2 }')
    print s.execute()

