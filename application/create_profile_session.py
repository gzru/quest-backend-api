from profile_session_base import ProfileQueryBase, ProfileSessionBase
import json


class CreateProfileQuery(ProfileQueryBase):

    def __init__(self):
        self.facebook_user_id = None
        self.facebook_access_token = None
        self.name = None
        self.friends = None
        self.meta_blob = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.facebook_user_id = self._get_required_str(tree, 'facebook_user_id')
        self.facebook_access_token = self._get_required_str(tree, 'facebook_access_token')
        self.name = self._get_optional_str(tree, 'name')
        self.meta_blob = self._get_optional_blob(tree, 'meta_blob')
        self.friends = self._parse_friends(tree)


class CreateProfileSession(ProfileSessionBase):

    def parse_query(self, data):
        self._query = CreateProfileQuery()
        self._query.parse(data)

    def execute(self):
        user_id = self._gen_user_id(self._query)
        if self._check_exists(user_id):
            raise Exception('User already exists')

        self._put_meta(user_id, self._query)
        self._put_friends(user_id, self._query)
        self._put_external_link(user_id, self._query.facebook_user_id)
        self._put_info(user_id, self._query)

        return json.dumps({ 'user_id': user_id })

