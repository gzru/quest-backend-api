from sessions.session import POSTSession
from core.query import Query
from clusters_searcher import ClastersSearcher, ClastersSearchParams
import logging


class Params(object):

    def __init__(self):
        self.searcher_params = ClastersSearchParams()

    def parse(self, query):
        user_token = query.get_user_token()
        self.searcher_params.user_id = user_token.user_id

        map_view = query.get_required('map_view')
        map_center = map_view.get_required('center')
        self.searcher_params.center_latitude = map_center.get_required_float64('latitude')
        self.searcher_params.center_longitude = map_center.get_required_float64('longitude')

        map_span = map_view.get_required('span')
        self.searcher_params.span_latitude = map_span.get_required_float64('latitude')
        self.searcher_params.span_longitude = map_span.get_required_float64('longitude')

        screen = query.get_required('screen')
        self.searcher_params.screen_width = screen.get_required_int64('width')
        self.searcher_params.screen_height = screen.get_required_int64('height')

        self.searcher_params.grid_size = query.get_required_int64('grid_size')
        self.searcher_params.signs_sample_size = query.get_optional_int64('signs_sample_size')


class MapSignsClustersSession(POSTSession):

    def __init__(self, global_context):
        self._searcher = ClastersSearcher(global_context.aerospike_connector)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        clusters = self._searcher.search(self._params.searcher_params)

        clusters_result = list()
        for cluster in clusters:
            signs = list()
            for sign_id in cluster.signs:
                sign = {
                    'sign_id': sign_id
                }
                signs.append(sign)

            entry = {
                'latitude': cluster.latitude,
                'longitude': cluster.longitude,
                'total_size': cluster.size,
                'signs': list(signs)
            }
            clusters_result.append(entry)

        logging.info('found %d clusters', len(clusters_result))

        result = {
            'success': True,
            'clusters': clusters_result
        }
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = MapSignsClustersSession(global_context)
    #s.parse_query('{"user_token": "333391931385052725", "map_view":{"center": {"latitude":55, "longitude":35}, "span": {"latitude": 5, "longitude": 5}}, "grid_size": 100, "screen": {"width":375, "height": 667}, "signs_sample_size": 2}')
    s.parse_query('{"screen":{"width":750,"height":1334},"user_token":"Y/m0jhTkvNFuzuHkF/e5nFZp6lmYJHhjwbIVQhSO0ASzgSzboTnEgLdOKm8I/aoNsSw6jXy1Jv+i5L9izsDkDgJ6KumnlfOcPofCQkOldaIPhNptiVDtoG/K409yVvhq","map_view":{"center":{"longitude":37.930229187011719,"latitude":55.793952941894531},"span":{"longitude":0.0012035096297040582,"latitude":0.0012035096297040582}},"grid_size":75}')
    print s.execute()

