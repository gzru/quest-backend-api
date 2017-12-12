from error import APIAccessError
from signs_engine import SignsEngine


class AccessRules(object):

    def __init__(self, aerospike_connector):
        self._signs_engine = SignsEngine(aerospike_connector)

    def check_can_read_sign(self, access_token, sign_id=None, sign_info=None):
        if access_token.is_admin == True:
            return True

        # Retrieve SignInfo if not presented
        if sign_info == None:
            sign_info = self._signs_engine.get_info(sign_id)

        if sign_info.is_private == False or sign_info.user_id == access_token.user_id:
            return True

        # Check access table
        if self._signs_engine.check_access(access_token.user_id, sign_info.sign_id) == True:
            return True

        raise APIAccessError('User {} has no read access to sign {}'.format(access_token.user_id, sign_info.sign_id))

    def check_can_edit_sign(self, access_token, sign_id=None, sign_info=None):
        if access_token.is_admin == True:
            return True

        # Retrieve SignInfo if not presented
        if sign_info == None:
            sign_info = self._signs_engine.get_info(sign_id)

        if sign_info.user_id == access_token.user_id:
            return True

        raise APIAccessError('User {} has no edit access to sign {}'.format(access_token.user_id, sign_info.sign_id))

    def check_can_view_private_info(self, access_token, user_id):
        if access_token.is_admin == True or access_token.user_id == user_id:
            return True

        raise APIAccessError('User {} has no access to private user info {}'.format(access_token.user_id, user_id))

    def check_can_edit_user_info(self, access_token, user_id):
        if access_token.is_admin == True or access_token.user_id == user_id:
            return True

        raise APIAccessError('User {} has no access to edit user info {}'.format(access_token.user_id, user_id))

