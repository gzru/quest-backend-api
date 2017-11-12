from query import Query
from users_engine import UsersEngine
import json


class SearchProfileQuery(Query):

    def __init__(self):
        self.keywords = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.keywords = self._get_required_str(tree, 'keywords')


class SearchProfileSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = SearchProfileQuery()
        self._query.parse(data)

    def execute(self):
        found = self._users_engine.search(self._query.keywords)

        result = {
            'success': True,
            'data': found
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = SearchProfileSession(global_context)
    s.parse_query('{"keywords": "asd"}')
    print s.execute()

