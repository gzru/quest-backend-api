from query import Query
from users_engine import UsersEngine, UserInfo
import json
import requests
import logging


class FacebookAuthQuery(Query):

    def __init__(self):
        self.access_token = None
        self.facebook_user_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.access_token = self._get_required_str(tree, 'facebook_access_token')
        self.facebook_user_id = self._get_required_str(tree, 'facebook_user_id')

        logging.info('facebook_access_token: {}'.format(self.access_token))
        logging.info('facebook_user_id: {}'.format(self.facebook_user_id))


class FacebookAuthSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._avatar_generator = global_context.avatar_generator
        self._facebook_profile_uri = 'https://graph.facebook.com/me'
        self._timeout_sec = 1

    def parse_query(self, data):
        self._query = FacebookAuthQuery()
        self._query.parse(data)

    def execute(self):
        facebook_prof = self._get_facebook_profile(self._query.access_token)
        facebook_user_id = facebook_prof['id']
        facebook_name = facebook_prof.get('name')

        # Check fb user id
        if self._query.facebook_user_id != facebook_user_id:
            raise Exception('facebook_user_id mismatch')

        # Check user exists
        user_id = self._users_engine.external_to_local_id(facebook_user_id)
        if user_id == None:
            user_id = self._create_new_user(facebook_name, facebook_user_id, self._query.access_token)

        result = {
            'success': True,
            'user_token': str(user_id),
            'user_id': int(user_id)
        }
        return json.dumps(result)

    def _get_facebook_profile(self, access_token):
        url = '{}?access_token={}'.format(self._facebook_profile_uri, access_token)
        try:
            resp = requests.get(url, timeout=self._timeout_sec)
        except Exception as ex:
            logging.error(ex)
            raise Exception('Request to facebook failed')

        profile = json.loads(resp.text)
        if 'id' not in profile:
            logging.error('Facebook bad response: {}'.format(profile))
            raise Exception('Request to facebook failed. Bad response.')
        return profile

    def _create_new_user(self, name, facebook_user_id, facebook_access_token):
        info = UserInfo()
        info.facebook_user_id = facebook_user_id
        info.facebook_access_token = facebook_access_token
        info.name = name

        # Generate user id
        info.user_id = self._users_engine.gen_user_id(info)

        # Create profile
        self._users_engine.put_info(info)
        self._users_engine.put_external_link(info.user_id, facebook_user_id)

        picture = self._avatar_generator.generate(name)
        self._users_engine.put_picture(info.user_id, picture)

        return info.user_id

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

