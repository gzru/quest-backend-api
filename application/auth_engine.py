from cipher import AESCipher
import smtplib
import random
import json
import logging


class AuthCode(object):

    def __init__(self):
        self.code = 0
        self.email = None
        self.expiration_timestamp = 0

    def encode(self, secret):
        data = json.dumps({'c': self.code, 'em': self.email, 'et': self.expiration_timestamp})
        coder = AESCipher(secret)
        return coder.encrypt(data)

    def decode(self, secret, cipher):
        try:
            coder = AESCipher(secret)
            data = coder.decrypt(cipher)
            parsed = json.loads(data)
            self.code = parsed.get('c')
            self.email = parsed.get('em')
            self.expiration_timestamp = parsed.get('et')
        except Exception as ex:
            logging.error(ex)
            raise Exception('Can\'t decode auth code')


class AuthEngine(object):

    def __init__(self):
        self._aes_secret = 'ifysvmukVlGYBQjQwY6J4bdi'
        self._robot_address = 'd.vasilyev@aiarlabs.com'
        self._robot_passwd = 'SVaQ7iEcnE'

    def auth_by_email_stage1(self, user_email):
        code = AuthCode()
        code.code = random.randint(1000, 9999)
        code.email = user_email
        code.expiration_timestamp = 0
        code_cipher = code.encode(self._aes_secret)

        self._send_code(user_email, code.code)

        return code_cipher

    def auth_by_email_stage2(self, code, code_cipher):
        auth_code = AuthCode()
        auth_code.decode(self._aes_secret, code_cipher)

        if auth_code.code != code:
            raise Exception('Inconsistent code')

        return auth_code.email

    def _send_code(self, user_email, auth_code):
        try:
            msg = "\r\n".join([
                "Subject: Quest authentication",
                "",
                "Your authentication code: {}".format(auth_code)
            ])

            server = smtplib.SMTP('smtp.gmail.com')
            server.ehlo()
            server.starttls()
            server.login(self._robot_address, self._robot_passwd)
            server.sendmail(self._robot_address, user_email, msg)
            server.quit()
        except Exception as ex:
            logging.error(ex)
            raise Exception('Can\'t send email with auth code to {}'.format(user_email))

