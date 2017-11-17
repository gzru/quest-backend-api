import math


def distance(lat1, lon1, lat2, lon2):
    EARTH_RADIUS = 6371.0

    lat1_rad = lat1 * math.pi / 180.0
    lon1_rad = lon1 * math.pi / 180.0
    lat2_rad = lat2 * math.pi / 180.0
    lon2_rad = lon2 * math.pi / 180.0
    k = math.sin(lat1_rad) * math.sin(lat2_rad) + \
        math.cos(lat1_rad) * math.cos(lat2_rad) * math.cos(lon1_rad - lon2_rad)
    return math.acos(k) * EARTH_RADIUS * 1000

