from query import Query, BadQuery
from PIL import Image
from io import BytesIO
import json
import base64


class PreviewSignQuery(Query):

    def __init__(self):
        self.user_id = None
        self.sign_id = None
        self.height = None
        self.width = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_optional_int64(tree, 'user_id')
        self.sign_id = self._get_required_int64(tree, 'sign_id')
        self.height = self._get_optional_int64(tree, 'height')
        self.width = self._get_optional_int64(tree, 'width')


class PreviewSignSession:

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._namespace = 'test'
        self._preview_set = 'sign_preview'

    def parse_query(self, data):
        self._query = PreviewSignQuery()
        self._query.parse(data)

    def execute(self):
        result = dict()
        result['preview_blob'] = self._get_preview(self._query.sign_id, self._query.height, self._query.width)

        return json.dumps(result)

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

