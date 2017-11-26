from query import Query
from clusters_searcher import ClastersSearcher, ClastersSearchParams
import json
import logging


class MapSignsClustersQuery(Query):

    def __init__(self):
        self.searcher_params = ClastersSearchParams()

    def parse(self, data):
        tree = self._parse_json(data)

        user_token = self._get_required_str(tree, 'user_token')
        # TODO
        try:
            self.searcher_params.user_id = int(user_token)
        except:
            raise Exception('Bad token')

        map_view = self._get_required(tree, 'map_view')
        map_center = self._get_required(map_view, 'center')
        self.searcher_params.center_latitude = self._get_required_float64(map_center, 'latitude')
        self.searcher_params.center_longitude = self._get_required_float64(map_center, 'longitude')

        map_span = self._get_required(map_view, 'span')
        self.searcher_params.span_latitude = self._get_required_float64(map_span, 'latitude')
        self.searcher_params.span_longitude = self._get_required_float64(map_span, 'longitude')

        screen = self._get_required(tree, 'screen')
        self.searcher_params.screen_width = self._get_required_int64(screen, 'width')
        self.searcher_params.screen_height = self._get_required_int64(screen, 'height')

        self.searcher_params.grid_size = self._get_required_int64(tree, 'grid_size')
        self.searcher_params.signs_sample_size = self._get_optional_int64(tree, 'signs_sample_size')


class MapSignsClustersSession(object):

    def __init__(self, global_context):
        self._searcher = ClastersSearcher(global_context.aerospike_connector)

    def parse_query(self, data):
        self._query = MapSignsClustersQuery()
        self._query.parse(data)

    def execute(self):
        clusters = self._searcher.search(self._query.searcher_params)

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
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = MapSignsClustersSession(global_context)
    #s.parse_query('{"user_token": "333391931385052725", "map_view":{"center": {"latitude":55, "longitude":35}, "span": {"latitude": 5, "longitude": 5}}, "grid_size": 100, "screen": {"width":375, "height": 667}, "signs_sample_size": 2}')
    #s.parse_query('{"screen":{"height": 1334, "width": 750}, "map_view":{"center":{"latitude": 54.92512512207031, "longitude": 20.15011787414551}, "span":{"latitude": 0.0055807535536587238, "longitude": 0.0055807535536587238}}, "user_token": "6914063709439103963", "grid_size": 75}')
    s.parse_query('{"screen": {"width": 750,"height": 1334},"user_token": "6914063709439103963","map_view": {"center": {"longitude": 4.8072772026062012,"latitude": 52.359981536865234},"span": {"longitude": 0.0024630629923194647,"latitude": 0.0024630629923194647}},"grid_size": 75}')
    print s.execute()

