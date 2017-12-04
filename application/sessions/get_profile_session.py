from sessions.session import POSTSession
from core.query import Query
from error import APILogicalError
from users_engine import UsersEngine, UserInfo
import logging
import requests


class Params(object):

    def __init__(self):
        self.user_id = None
        self.properties = None

    def parse(self, query):
        self.user_id = query.get_required_int64('user_id')
        self.properties = self._get_properties(query)

    def _get_properties(self, query):
        properties = query.get_optional_list('properties')
        if properties == None:
            return list()

        for value in properties:
            Query.check_str('properties_value', value)

        return properties


class GetProfileSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        info = self._users_engine.get_info(self._params.user_id)
        if info == None:
            raise APILogicalError('User not found')

        # TODO remove facebook_access_token from result
        result = {
            'user_id': info.user_id,
            'facebook_user_id': info.facebook_user_id,
            'facebook_access_token': info.facebook_access_token,
            'name': info.name,
            'username': info.username,
            'email': info.email
        }

        if 'meta_blob' in self._params.properties:
            result['meta_blob'] =  self._users_engine.get_meta(self._params.user_id)

        if 'picture_blob' in self._params.properties:
            result['picture_blob'] =  self._users_engine.get_picture(self._params.user_id)

        if 'friends' in self._params.properties:
            facebook_user_id = result['facebook_user_id']
            facebook_access_token = result['facebook_access_token']
            if facebook_user_id != None and facebook_access_token != None:
                result['friends'] = self._get_friends_fb(info.user_id, facebook_user_id, facebook_access_token)
            else:
                result['friends'] = list()

        return result

    def _get_friends_fb(self, user_id, facebook_user_id, facebook_access_token):
        logging.info('_get_friends_fb: Retrieve friends from facebook, facebook_user_id = {}'.format(facebook_user_id))

        friends = list()
        next_page_uri = 'https://graph.facebook.com/{}/friends?access_token={}'.format(facebook_user_id, facebook_access_token)
        while next_page_uri:
            try:
                resp = requests.get(next_page_uri)
                # parse
                parsed = json.loads(resp.text)
                # take profiles
                data = parsed.get('data')
                for entry in data:
                    facebook_user_id = entry.get('id')

                    info = self._get_user_by_fb(facebook_user_id)
                    if info == None:
                        continue

                    profile = {
                        'user_id': info.user_id,
                        'facebook_user_id': info.facebook_user_id,
                        'name': info.name,
                        'username': info.username,
                        'email': info.email
                    }

                    relation = self._users_engine.get_relations_many(user_id, [info.user_id])[0];
                    if relation != None:
                        profile['twilio_channel'] = relation.twilio_channel
                    else:
                        profile['twilio_channel'] = None

                    friends.append(profile)

                # next page
                paging = parsed.get('paging')
                if paging:
                    next_page_uri = paging.get('next')
                else:
                    next_page_uri = None
            except Exception as ex:
                logging.error(ex)
                raise

        logging.info('_get_friends_fb: Got {} entries'.format(len(friends)))
        return friends

    def _get_user_by_fb(self, facebook_user_id):
        user_id = self._users_engine.external_to_local_id(facebook_user_id)
        if user_id == None:
            logging.warning('Can\'t find user by facebook user id')

            info = UserInfo()
            info.user_id = self._gen_user_id(info)
            info.facebook_user_id = facebook_user_id

            # Create profile
            self._users_engine.put_info(info.user_id, info)
            self._users_engine.put_external_link(info.user_id, facebook_user_id)
        else:
            info = self._users_engine.get_info(user_id)
        return info


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetProfileSession(global_context)
    s.parse_query('{"user_id": 8618994807331250316, "properties": ["friends"]}')
    print s.execute()

