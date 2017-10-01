from query import Query
from profile_session_base import ProfileSessionBase
import json


class SearchProfileQuery(Query):

    def __init__(self):
        self.keywords = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.keywords = self._get_required_str(tree, 'keywords')


class SearchProfileSession(ProfileSessionBase):

    def parse_query(self, data):
        self._query = SearchProfileQuery()
        self._query.parse(data)

    def execute(self):
        found = self._engine.search(self._query.keywords)

        result = {
            'success': True,
            'data': found
        }
        return json.dumps(result)

