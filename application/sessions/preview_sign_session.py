from sessions.session import POSTSession
from config import Config
from PIL import Image
from io import BytesIO
import base64


class Params(object):

    def __init__(self):
        self.sign_id = None
        self.height = None
        self.width = None

    def parse(self, query):
        self.sign_id = query.get_required_int64('sign_id')
        self.height = query.get_optional_int64('height')
        self.width = query.get_optional_int64('width')


class PreviewSignSession(POSTSession):

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._namespace = Config.AEROSPIKE_NS_SIGNS
        self._preview_set = 'sign_preview'
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        result = {
            'preview_blob': self._get_preview(self._params.sign_id, self._params.height, self._params.width)
        }

        return result

    def _get_preview(self, sign_id, height, width):
        key = (self._namespace, self._preview_set, str(sign_id))
        data = self._aerospike_connector.get_data(key)
        if not data:
            return None

        buf = BytesIO()

        img = Image.open(BytesIO(data))
        if not height:
            height = img.size[0]
        if not width:
            width = img.size[1]
        img.thumbnail((height, width))
        img.save(buf, format='JPEG')

        return base64.b64encode(buf.getvalue())

