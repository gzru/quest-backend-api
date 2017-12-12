from sessions.session import POSTSession
from core.query import Query
from config import Config
from error import APIInternalServicesError
from PIL import Image
from io import BytesIO
import json
import base64
import multiprocessing.dummy
import requests
import logging


class Params(object):

    def __init__(self):
        self.user_id = None
        self.signs = None
        self.screen_blob = None

    def parse(self, query):
        self.user_id = query.get_optional_int64('user_id')
        self.signs = self._get_signs(query)
        self.screen_blob = query.get_required_blob('image_screen')

    def _get_signs(self, query):
        signs = query.get_required_list('signs')

        for sign_id in signs:
            Query.check_float('sign_id', sign_id)

        return signs


class GetMatchingSession(POSTSession):

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._namespace = Config.AEROSPIKE_NS_SIGNS
        self._image_set = 'sign_image'
        # TODO use all matchers
        self._matcher_host = Config.MATCHER_HOSTS[0]
        self._matcher_api_handler = '/api/matching/get'
        self._timeout_sec = Config.MATCHER_TIMEOUT_SEC
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        logging.info('Get matchings for {} signs'.format(len(self._params.signs)))
        results = self._send_requests(self._gen_matching_requests(self._params))
        return { 'matches': results }

    def _gen_matching_requests(self, params):
        image_screen_b64 = self._prepare_image_data(params.screen_blob)
        for sign_id in params.signs:
            yield (sign_id, image_screen_b64)

    def _prepare_image_data(self, data):
        buf = BytesIO()
        img = Image.open(BytesIO(data))

        logging.info('Prepare image, base size {}'.format(img.size))

        img.thumbnail((400, 400))
        img.save(buf, format='JPEG')
        return base64.b64encode(buf.getvalue())

    def _send_one_request(self, (sign_id, image_screen_b64)):
        # get
        image_background_data = self._aerospike_connector.get_data((self._namespace, self._image_set, str(sign_id)))
        if image_background_data == None:
            logging.warning('Background image not found, sing_id = {}'.format(sign_id))
            return { 'error': 'Background image not found' }
        image_background_b64 = self._prepare_image_data(image_background_data)
        # send query
        try:
            request = json.dumps({'image_screen': image_screen_b64, 'image_background': image_background_b64})

            url = 'https://' + self._matcher_host + self._matcher_api_handler
            resp = requests.post(url, data=request, timeout=self._timeout_sec)
        except Exception as ex:
            logging.error(ex)
            raise APIInternalServicesError('matching/get request failed')
        # parse response
        try:
            result = {
                'sign_id': sign_id,
                'transformation': json.loads(resp.text)
            }
            return result
        except Exception as ex:
            logging.error('{}\n{}'.format(ex, resp.text[:100]))
            raise APIInternalServicesError('matching/get request failed, bad response')

    def _send_requests(self, requests_gen):
        pool = multiprocessing.dummy.Pool(5)
        result = pool.map(self._send_one_request, requests_gen)
        pool.close()
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    with open('/tmp/test_sign.jpg', 'r') as myfile:
        data = base64.b64encode(myfile.read())
    query = {
        'signs': [1, 2, 3],
        'image_screen': data
    }

    s = GetMatchingSession(global_context)
    #s._prepare_image_data(base64.b64decode(data))
    #s._send_one_request((1, data))
    s.parse_query(json.dumps(query))
    print s.execute()

