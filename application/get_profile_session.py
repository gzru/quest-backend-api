from profile_session_base import UserInfo, ProfileQueryBase, ProfileSessionBase
import json
import logging
import requests


class GetProfileQuery(ProfileQueryBase):

    def __init__(self):
        self.user_id = None
        self.properties = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_required_int64(tree, 'user_id')
        self.properties = self._get_properties(tree)

    def _get_properties(self, tree):
        properties = tree.get('properties')
        if not properties:
            return list()

        if not isinstance(properties, list):
            raise BadQuery('"properties" have bad format')

        for value in properties:
            self._check_str('properties_value', value)

        return properties


class GetProfileSession(ProfileSessionBase):

    def parse_query(self, data):
        self._query = GetProfileQuery()
        self._query.parse(data)

        logging.error('user_id = {}'.format(self._query.user_id))

    def execute(self):
        result = self._get_info(self._query.user_id)

        if 'meta_blob' in self._query.properties:
            result['meta_blob'] =  self._get_meta(self._query.user_id)

        if 'friends' in self._query.properties:
            #result['friends'] =  self._get_friends(self._query.user_id)
            result['friends'] =  self._get_friends_fb(result['facebook_user_id'], result['facebook_access_token'])

        return json.dumps(result)

    def _get_friends_fb(self, facebook_user_id, facebook_access_token):
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
                    print entry
                    facebook_user_id = entry.get('id')

                    profile = dict()
                    profile['facebook_user_id'] = facebook_user_id
                    profile['user_id'] = self._get_user_id_by_fb(facebook_user_id)
                    profile['name'] = entry.get('name')

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
        return friends

    def _get_user_id_by_fb(self, facebook_user_id):
        user_id = self._get_external_link(facebook_user_id)
        if not user_id:
            logging.warning('Can\'t find user by facebook user id')

            info = UserInfo()
            info.facebook_user_id = facebook_user_id

            # Generate user id
            info.user_id = self._gen_user_id(info)

            # Create profile
            self._put_info(info.user_id, info)
            self._put_external_link(info.user_id, facebook_user_id)

        return int(user_id)

