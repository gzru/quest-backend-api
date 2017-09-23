from query import Query, BadQuery
from profile_session_base import UserInfo, ProfileSessionBase
import json
import requests
import logging


class FacebookAuthQuery(Query):

    def __init__(self):
        self.access_token = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.access_token = self._get_required_str(tree, 'facebook_access_token')
        self.facebook_user_id = self._get_required_str(tree, 'facebook_user_id')

        logging.error('facebook_access_token: {}'.format(self.access_token))
        logging.error('facebook_user_id: {}'.format(self.facebook_user_id))


class FacebookAuthSession(ProfileSessionBase):

    def __init__(self, global_context):
        super(FacebookAuthSession, self).__init__(global_context)
        self._facebook_profile_uri = 'https://graph.facebook.com/me'
        self._timeout_sec = 1

    def parse_query(self, data):
        self._query = FacebookAuthQuery()
        self._query.parse(data)

    def execute(self):
        url = '{}?access_token={}'.format(self._facebook_profile_uri, self._query.access_token)
        try:
            resp = requests.get(url, timeout=self._timeout_sec)
        except Exception as ex:
            logging.error(ex)
            raise Exception('Request to facebook failed')

        facebook_prof = json.loads(resp.text)
        facebook_user_id = facebook_prof['id']

        # Check fb user id
        if self._query.facebook_user_id != facebook_user_id:
            raise Exception('facebook_user_id mismatch')

        # Check user exists
        user_id = self._get_external_link(facebook_user_id)
        if not user_id:
            info = UserInfo()
            info.facebook_user_id = facebook_user_id
            info.facebook_access_token = self._query.access_token
            info.name = facebook_prof['name']

            # Generate user id
            user_id = self._gen_user_id(info)

            # Create profile
            self._put_info(user_id, info)
            self._put_external_link(user_id, facebook_user_id)

        return json.dumps({ 'user_token': str(user_id), 'user_id': int(user_id) })

"""
from requests_oauthlib import OAuth2Session
from requests_oauthlib.compliance_fixes import facebook_compliance_fix

self._client_id = '2016606938561183'
self._client_secret = '823f5fa526a109e4be9dc0c044d96617'
self._redirect_uri = 'http://api.quest.aiarlabs.com/api/auth/facebook_redirect'
self._redirect_uri_https = 'https://api.quest.aiarlabs.com/api/auth/facebook_redirect'
self._token_url = 'https://graph.facebook.com/oauth/access_token'

def parse_facebook_auth_code(code):
    redirect_response='{}?code={}'.format(self._redirect_uri_https, code)

    facebook = OAuth2Session(self._client_id, redirect_uri=self._redirect_uri)
    facebook = facebook_compliance_fix(facebook)

    # Fetch the access token
    token = facebook.fetch_token(self._token_url, client_secret=self._client_secret,
                    authorization_response=redirect_response)

    return token


def make_authorization_url():
    client_id = '2016606938561183'
    authorization_base_url = 'https://www.facebook.com/dialog/oauth'
    redirect_uri = 'http://api.quest.aiarlabs.com/api/auth/facebook_redirect'

    facebook = OAuth2Session(client_id, redirect_uri=redirect_uri)
    facebook = facebook_compliance_fix(facebook)

    # Redirect user to Facebook for authorization
    authorization_url, state = facebook.authorization_url(authorization_base_url)
    return authorization_url


if __name__ == "__main__":
    print 'Please go here and authorize,', make_authorization_url()
"""

