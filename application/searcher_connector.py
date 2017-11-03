from error import APILogicalError, APIInternalServicesError
import logging
import requests
import json


class SearcherMapClustersQParams(object):

    def __init__(self):
        self.map_center_latitude = None
        self.map_center_longitude = None
        self.map_span_latitude = None
        self.map_span_longitude = None
        self.screen_width = None
        self.screen_height = None
        self.grid_size = None
        self.signs_sample_size = None


class SearcherConnector(object):

    def __init__(self):
        self._searcher_host = '174.138.38.144'
        self._searcher_port = 28000
        self._searcher_req_timeout = 0.1

    # TODO: move args into struct
    def search_signs(self, user_id, lat, lon, radius, max_n, min_rank, sort_by, debug, features):
        logging.info('search_signs: Retrieve signs')
        try:
            request = {
                'user_id': user_id,
                'latitude': lat,
                'longitude': lon,
                'radius': radius,
                'max_n': max_n,
                'min_rank': min_rank,
                'features': features
            }
            if sort_by != None:
                request['sort_by'] = sort_by
            if debug != None:
                request['debug'] = debug

            url = 'http://{}:{}/api/sign/search'.format(self._searcher_host, self._searcher_port)
            resp = requests.post(url, data=json.dumps(request), timeout=self._searcher_req_timeout)
            result = json.loads(resp.text)
        except Exception as ex:
            logging.error('Searcher search request failed: {}'.format(ex))
            raise APIInternalServicesError('Searcher search request failed: {}'.format(ex))

        signs = result.get('signs')
        if signs == None:
            raise APIInternalServicesError('Searcher search request failed: Bad response: {}'.format(resp.text))

        debug = result.get('debug')

        logging.info('search_signs: Got {} signs'.format(len(signs)))
        return signs, debug

    def get_map_clusters(self, params):
        if not isinstance(params, SearcherMapClustersQParams):
            raise Exception('Bad params type')

        logging.info('get_map_clusters: Retrieve signs clusters')
        try:
            request = {
                'map_view': {
                    'center': {
                        'latitude': params.map_center_latitude,
                        'longitude': params.map_center_longitude
                    },
                    'span': {
                        'latitude': params.map_span_latitude,
                        'longitude': params.map_span_longitude
                    }
                },
                'screen': {
                    'width': params.screen_width,
                    'height': params.screen_height
                },
                'grid_size': params.grid_size,
                'signs_sample_size': params.signs_sample_size
            }

            url = 'http://{}:{}/api/sign/clustering/map'.format(self._searcher_host, self._searcher_port)
            resp = requests.post(url, data=json.dumps(request), timeout=self._searcher_req_timeout)
            result = json.loads(resp.text)
        except Exception as ex:
            logging.error('Searcher request failed: {}'.format(ex))
            raise APIInternalServicesError('Searcher request failed: {}'.format(ex))

        clusters = result.get('clusters')
        if clusters == None:
            raise APIInternalServicesError('Searcher request failed: Bad response: {}'.format(resp.text))

        logging.info('get_map_clusters: Got {} clusters'.format(len(clusters)))
        return clusters


if __name__ == "__main__":
    connector = SearcherConnector()

    params = SearcherMapClustersQParams()
    params.map_center_latitude = 55.7558
    params.map_center_longitude = 37.6173
    params.map_span_latitude = 5
    params.map_span_longitude = 20
    params.screen_width = None
    params.screen_height = None
    params.grid_size = 100

    print connector.get_map_clusters(params)

