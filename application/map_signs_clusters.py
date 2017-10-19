from query import Query
from signs_engine import SignsEngine, SearcherMapClustersQParams
import json


class MapSignsClustersQuery(Query):

    def __init__(self):
        self.searcher_params = SearcherMapClustersQParams()

    def parse(self, data):
        tree = self._parse_json(data)

        map_view = self._get_required(tree, 'map_view')
        map_center = self._get_required(map_view, 'center')
        self.searcher_params.map_center_latitude = self._get_required_float64(map_center, 'latitude')
        self.searcher_params.map_center_longitude = self._get_required_float64(map_center, 'longitude')

        map_span = self._get_required(map_view, 'span')
        self.searcher_params.map_span_latitude = self._get_required_float64(map_span, 'latitude')
        self.searcher_params.map_span_longitude = self._get_required_float64(map_span, 'longitude')

        screen = self._get_required(tree, 'screen')
        self.searcher_params.screen_width = self._get_required_int64(screen, 'width')
        self.searcher_params.screen_height = self._get_required_int64(screen, 'height')

        self.searcher_params.grid_size = self._get_required_int64(tree, 'grid_size')


class MapSignsClustersSession(object):

    def __init__(self, global_context):
        self._signs_engine = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = MapSignsClustersQuery()
        self._query.parse(data)

    def execute(self):
        clusters = self._signs_engine.get_signs_clusters(self._query.searcher_params)

        result = {
            'success': True,
            'clusters': clusters
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = MapSignsClustersSession(global_context)
    s.parse_query('{"map_view":{"center": {"latitude":55.7558, "longitude":37.6173}, "span": {"latitude": 5, "longitude": 20}}, "grid_size": 100, "screen": {"width":375, "height": 667}}')
    print s.execute()

