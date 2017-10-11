from query import Query, BadQuery
from signs_engine import SignsEngine
import json


class SearchSignsQuery(Query):

    def __init__(self):
        self.user_id = None
        self.latitude = None
        self.longitude = None
        self.radius = None
        self.max_n = None
        self.min_rank = None
        self.sort_by = None
        self.debug = None
        self.features = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = int(self._get_required_str(tree, 'user_token'))
        self.latitude = self._get_required_float64(tree, 'latitude')
        self.longitude = self._get_required_float64(tree, 'longitude')
        self.radius = self._get_required_float64(tree, 'radius')
        self.features = self._parse_features(tree)
        self.sort_by = self._get_optional_str(tree, 'sort_by')
        self.debug = self._get_optional_bool(tree, 'debug')

        self.max_n = self._get_optional_int64(tree, 'max_n')
        if self.max_n == None:
            self.max_n = 100

        self.min_rank = self._get_optional_float64(tree, 'min_rank')
        if self.min_rank == None:
            self.min_rank = 0

    def _parse_features(self, tree):
        features = self._get_optional(tree, 'features')
        if features == None:
            return list()

        if not isinstance(features, list):
            raise BadQuery('"features" have bad format')

        for value in features:
            self._check_float('features_value', value)

        return features


class SearchSignsSession(object):

    def __init__(self, global_context):
        self._signs_engine = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = SearchSignsQuery()
        self._query.parse(data)

    def execute(self):
        found, debug = self._signs_engine.search_signs(self._query.user_id, \
                                            self._query.latitude, \
                                            self._query.longitude, \
                                            self._query.radius, \
                                            self._query.max_n, \
                                            self._query.min_rank, \
                                            self._query.sort_by, \
                                            self._query.debug, \
                                            self._query.features)

        result = {
            'success': True,
            'signs': found,
            'debug': debug
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = SearchSignsSession(global_context)
    s.parse_query('{"latitude":54.713336944580078,"features":[],"debug":false,"radius":100.67225646972656,"min_rank":0.80000001192092896,"longitude":20.538284301757812,"user_token":"570414123406147177"}')
    print s.execute()


