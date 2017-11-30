from error import APIAccessError, APIUserTokenError
from core.cipher import AESCipher
import json
import time
import logging


class AccessToken(object):

    AES_SECRET = 'e4Apu7rkVNWwmpheCu746Z9q'

    def __init__(self):
        self.user_id = None
        self.timestamp = None
        self.is_admin = False

    def __str__(self):
        data = {
            'user_id': self.user_id,
            'timestamp': self.timestamp,
            'is_admin': self.is_admin
        }
        return str(data)

    def encode(self):
        data = {
            'user_id': self.user_id,
            'timestamp': self.timestamp,
            'is_admin': self.is_admin
        }

        coder = AESCipher(AccessToken.AES_SECRET)
        return coder.encrypt(json.dumps(data))

    def decode(self, string):
        coder = AESCipher(AccessToken.AES_SECRET)
        raw_data = coder.decrypt(string)
        parsed_data = json.loads(raw_data)

        self.user_id = parsed_data['user_id']
        self.timestamp = parsed_data['timestamp']
        self.is_admin = parsed_data.get('is_admin', False)

    ''' Simple method to check access '''
    def check_access(self, user_id):
        if self.user_id == user_id or self.is_admin == True:
            return
        raise APIAccessError('User {} has no access to user {} data'.format(self.user_id, user_id))

    @classmethod
    def make_encoded(cls, user_id):
        token = cls()
        token.user_id = user_id
        token.timestamp = int(time.time())
        token.is_admin = False
        return token.encode()

    @classmethod
    def from_encoded(cls, string):
        try:
            token = cls()
            token.decode(string)
            return token
        except Exception as ex:
            logging.error('Failed to decode token string, %s', ex)
            raise APIUserTokenError('Failed to decode token string')


if __name__ == "__main__":
    token = AccessToken()
    token.user_id = 123
    string = token.encode()
    print string

    token2 = AccessToken()
    token2.decode(string)
    print token2

    string3 = AccessToken.make_encoded(2414917961944660396)
    print string3

    token3 = AccessToken.from_encoded(string3)
    print token3


