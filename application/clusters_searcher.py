from search_index import SearchIndex
import geo


class Cluster(object):

    def __init__(self):
        self.latitude = None
        self.longitude = None
        self.size = 0
        self.signs = set()


class ClustersBuilder(object):

    def __init__(self, gridsize_lat, gridsize_lon, n_max, n_signs):
        self.gridsize_lat = gridsize_lat
        self.gridsize_lon = gridsize_lon
        self.n_max = n_max
        self.n_signs = n_signs
        self.clusters = list()

    def put_sign(self, sign_id, latitude, longitude):
        nearest_cluster = None
        nearest_distance = float('inf')
        for cluster in self.clusters:
            if latitude > cluster.latitude + self.gridsize_lat or \
               latitude < cluster.latitude - self.gridsize_lat or \
               longitude > cluster.longitude + self.gridsize_lon or \
               longitude < cluster.longitude - self.gridsize_lon:
                continue

            distance = geo.distance(cluster.latitude, cluster.longitude, latitude, longitude)
            if distance < nearest_distance:
                nearest_cluster = cluster
                nearest_distance = distance

        if nearest_cluster:
            # Check duplicate
            if sign_id in nearest_cluster.signs:
                return
            if len(nearest_cluster.signs) < self.n_signs:
                nearest_cluster.signs.add(sign_id)
            nearest_cluster.size += 1
        elif len(self.clusters) < self.n_max:
            cluster = Cluster()
            cluster.latitude = latitude;
            cluster.longitude = longitude;
            cluster.size = 1;
            cluster.signs.add(sign_id)
            self.clusters.append(cluster)


class ClastersSearchParams(object):

    def __init__(self):
        self.user_id = 0
        self.center_latitude = 0
        self.center_longitude = 0
        self.span_latitude = 0
        self.span_longitude = 0
        self.screen_width = 375
        self.screen_height = 667
        self.grid_size = 40
        self.signs_sample_size = 100


class ClastersSearcher(object):

    def __init__(self, aerospike_connector):
        self._search_index = SearchIndex(aerospike_connector)

    def search(self, params):
        # Prepare params
        lat1 = params.center_latitude - params.span_latitude
        lon1 = params.center_longitude - params.span_longitude
        lat2 = params.center_latitude + params.span_latitude
        lon2 = params.center_longitude + params.span_longitude

        gridsize_lat = params.span_latitude * 2 / params.screen_height * params.grid_size
        gridsize_lon = params.span_longitude * 2 / params.screen_width * params.grid_size

        n_signs = params.signs_sample_size
        if n_signs == None:
            n_signs = 100

        # Builder to accumulate and group signs
        builder = ClustersBuilder(gridsize_lat, gridsize_lon, 100, n_signs)

        # Retrieve and process signs
        self._search_index.search_region(params.user_id, \
                                         lat1, lon1, \
                                         lat2, lon2, \
                                         self._records_processor(params, builder))
        return builder.clusters

    def _records_processor(self, params, builder):
        def _impl(record):
            sign_id = int(record['sign_id'])
            location = record['location'].unwrap().get('coordinates')
            builder.put_sign(sign_id, location[1], location[0])
        return _impl


if __name__ == "__main__":
    import aerospike_connector

    connector = aerospike_connector.AerospikeConnector(128000, 1000, 1000)
    connector.connect(['localhost'])

    params = ClastersSearchParams()
    params.user_id = 10001
    params.center_latitude = 1
    params.center_longitude = 1
    params.span_latitude = 0.5
    params.span_longitude = 0.5
    params.screen_width = 375
    params.screen_height = 667
    params.grid_size = 40
    params.signs_sample_size = 100

    cs = ClastersSearcher(connector)
    clusters = cs.search(params)
    for cluster in clusters:
        print cluster.latitude, cluster.longitude, cluster.signs

