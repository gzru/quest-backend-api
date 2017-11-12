from error import APIInternalServicesError
import settings
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import ChatGrant, IpMessagingGrant
from twilio.rest import Client
import logging


class TwilioConnector(object):

    def __init__(self):
        self._account_sid = settings.TWILIO_ACCOUNT_SID
        self._auth_token = settings.TWILIO_AUTH_TOKEN
        self._api_key = settings.TWILIO_API_KEY
        self._api_secret = settings.TWILIO_API_SECRET
        self._chat_service_sid = settings.TWILIO_CHAT_SERVICE_SID

    def make_messaging_token(self, user_id, uuid):
        try:
            identity = str(user_id)

            # Create access token with credentials
            token = AccessToken(self._account_sid, self._api_key, self._api_secret, identity=identity)

            # Create an Chat grant and add to token
            # TODO move into settings
            chat_grant = ChatGrant(service_sid=self._chat_service_sid, push_credential_sid='CR5776d4fff23e9bf0d85389af5c763eb3')
            token.add_grant(chat_grant)

            return token.to_jwt().decode('utf-8')
        except Exception as ex:
            logging.error('make_messaging_token failed, {}'.format(ex))
            raise APIInternalServicesError('make_messaging_token failed')

    def create_channel(self, user_id1, user_id2):
        try:
            # Initialize the client
            client = Client(self._account_sid, self._auth_token)

            # Create the channel
            channel = client.chat \
                    .services(self._chat_service_sid) \
                    .channels \
                    .create(friendly_name="", type="private")

            member1 = channel.members.create(str(user_id1))
            member2 = channel.members.create(str(user_id2))

            return channel.sid

        except Exception as ex:
            logging.error('create_channel failed, {}'.format(ex))
            raise APIInternalServicesError('create_channel failed')
"""
    def tt(self, user_id1):
        try:
            # Initialize the client
            client = Client(self._account_sid, self._auth_token)

            # Create the channel
            members = client.chat \
                    .services(self._chat_service_sid) \
                    .channels('CH57718fb2132b42cf83727524e00559f9') \
                    .members.list()
            for m in members:
                print m.identity

        except Exception as ex:
            logging.error('create_channel failed, {}'.format(ex))
            raise APIInternalServicesError('create_channel failed')
"""

if __name__ == "__main__":
    se = TwilioConnector()

    print se.make_messaging_token(123, 213)
    #print se.create_channel(123, 12)
    #print se.tt(123)
    #import twilio
    #print twilio.jwt.__file__

