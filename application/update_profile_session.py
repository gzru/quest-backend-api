from profile_session_base import ProfileQueryBase, ProfileSessionBase
import json


class UpdateProfileQuery(ProfileQueryBase):

    def __init__(self):
        self.user_id = None
        self.facebook_user_id = None
        self.facebook_access_token = None
        self.name = None
        self.friends = None
        self.meta_blob = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = int(self._get_required_str(tree, 'user_token'))
        self.facebook_user_id = self._get_optional_str(tree, 'facebook_user_id')
        self.facebook_access_token = self._get_optional_str(tree, 'facebook_access_token')
        self.name = self._get_optional_str(tree, 'name')
        self.meta_blob = self._get_optional_blob(tree, 'meta_blob')


class UpdateProfileSession(ProfileSessionBase):

    def parse_query(self, data):
        self._query = UpdateProfileQuery()
        self._query.parse(data)

    def execute(self):
        user_id = self._query.user_id
        if not self._check_exists(user_id):
            raise Exception('User not found')

        self._put_meta(user_id, self._query)
        self._put_friends(user_id, self._query)
        self._put_external_link(user_id, self._query.facebook_user_id)
        self._put_info(user_id, self._query)

        return json.dumps({ 'user_token': str(user_id) })

