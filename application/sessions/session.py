from core.query import Query
import json


class POSTSession(object):

    def parse_query(self, data):
        ''' Parse json '''
        query = Query.from_json(data)

        ''' Initialize session specific params '''
        self._init_session_params(query)

    def execute(self):
        result = self._run_session()
        return json.dumps(result)

    def _init_session_params(self, query):
        raise Exception('method _init_session_params was not implemented')

    def _run_session(self):
        raise Exception('method _run_session was not implemented')

