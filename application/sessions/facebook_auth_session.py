from sessions.session import POSTSession
from users_engine import UsersEngine, UserInfo
import requests
import logging


class Params(object):

    def __init__(self):
        self.access_token = None
        self.facebook_user_id = None

    def parse(self, query):
        self.access_token = query.get_required_str('facebook_access_token')
        self.facebook_user_id = query.get_required_str('facebook_user_id')


class FacebookAuthSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._avatar_generator = global_context.avatar_generator
        self._facebook_profile_uri = 'https://graph.facebook.com/me'
        self._timeout_sec = 1
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        facebook_prof = self._get_facebook_profile(self._params.access_token)
        facebook_user_id = facebook_prof['id']
        facebook_name = facebook_prof.get('name')

        # Check fb user id
        if self._params.facebook_user_id != facebook_user_id:
            raise Exception('facebook_user_id mismatch')

        # Check user exists
        user_id = self._find_user(facebook_user_id)
        if user_id == None:
            user_id = self._create_new_user(facebook_name, facebook_user_id, self._params.access_token)

        result = {
            'success': True,
            'user_token': str(user_id),
            'user_id': int(user_id)
        }
        return result

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

    def _find_user(self, facebook_user_id):
        user_id = self._users_engine.external_to_local_id(facebook_user_id)
        if user_id == None:
            return None
        try:
            info = self._users_engine.get_info(user_id)
            return info.user_id
        except:
            return None

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

