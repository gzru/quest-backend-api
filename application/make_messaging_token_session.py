from query import Query
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import ChatGrant
import settings
import json


class MakeMessagingTokenQuery(Query):

    def __init__(self):
        self.user_id = None
        self.uuid = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_required_int64(tree, 'user_id')
        self.sign_id = self._get_required_str(tree, 'uuid')


class MakeMessagingTokenSession:

    def __init__(self, global_context):
        self._account_sid = settings.TWILIO_ACCOUNT_SID
        self._api_key = settings.TWILIO_API_KEY
        self._api_secret = settings.TWILIO_API_SECRET
        self._chat_service_sid = settings.TWILIO_CHAT_SERVICE_SID

    def parse_query(self, data):
        self._query = MakeMessagingTokenQuery()
        self._query.parse(data)

    def execute(self):
        identity = '{}_{}'.format(self._query.user_id, self._query.uuid)

        # Create access token with credentials
        token = AccessToken(self._account_sid, self._api_key, self._api_secret, identity=identity)

        # Create an Chat grant and add to token
        chat_grant = ChatGrant(service_sid=self._chat_service_sid)
        token.add_grant(chat_grant)

        result = { 'token': token.to_jwt().decode('utf-8') }
        return json.dumps(result)

