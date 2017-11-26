from sessions.session import POSTSession
from core.query import Query
from searcher import Searcher, SearchParams
import logging


class Params(object):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.latitude = None
        self.longitude = None
        self.radius = None
        self.max_n = None
        self.min_rank = None
        self.sort_by = None
        self.debug = None
        self.features = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.user_id = query.get_optional_int64('user_id', self.user_token.user_id)
        self.latitude = query.get_required_float64('latitude')
        self.longitude = query.get_required_float64('longitude')
        self.radius = query.get_required_float64('radius')
        self.features = self._parse_features(query)
        self.sort_by = query.get_optional_str('sort_by')
        self.debug = query.get_optional_bool('debug')
        self.max_n = query.get_optional_int64('max_n', 100)
        self.min_rank = query.get_optional_float64('min_rank', 0)

    def _parse_features(self, query):
        features = query.get_optional_list('features')
        if features == None:
            return list()

        for value in features:
            Query.check_float('features_value', value)

        return features


class SearchSignsSession(POSTSession):

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        params = SearchParams()
        params.user_id = self._params.user_id
        params.max_n = self._params.max_n
        params.latitude = self._params.latitude
        params.longitude = self._params.longitude
        params.radius = self._params.radius
        params.min_rank = self._params.min_rank
        params.debug = self._params.debug
        params.features = self._params.features
        if self._params.sort_by == 'distance':
            params.sort_by = SearchParams.SORT_BY_DISTANCE
        elif self._params.sort_by == 'rank':
            params.sort_by = SearchParams.SORT_BY_RANK

        searcher = Searcher(self._aerospike_connector)
        found, debug = searcher.search(params)

        result = {
            'success': True,
            'signs': found,
            'debug': debug
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = SearchSignsSession(global_context)
    #s.parse_query('{"latitude":54.713336944580078,"features":[],"debug":true,"radius":100.67225646972656,"min_rank":0.80000001192092896,"longitude":20.538284301757812,"user_token":"123"}')
    s.parse_query('{"latitude": -35.6709, "longitude": -9.6504, "user_token": "jGoGqIqfqfB/eSCGDIabQ6FNLgKpERPeUanlbTJUYCXstTnyso2PZYZDExRJlSjWUwadpX3NWqQP7S4YS0baQmY1RHFNEWrMY6G5CDY427EUxwMBwDqDF1HtrePnWJxg", "radius": 1000}')
    print s.execute()


