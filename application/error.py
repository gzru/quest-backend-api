
class APIError(Exception):

    def __init__(self, message, code):
        super(APIError, self).__init__(message)
        self.code = code


class APIUserTokenError(APIError):

    def __init__(self, message):
        super(APIUserTokenError, self).__init__(message, 3)


class APIInconsistentAuthCodeError(APIError):

    def __init__(self, message):
        super(APIInconsistentAuthCodeError, self).__init__(message, 4)


class APIInternalServicesError(APIError):

    def __init__(self, message):
        super(APIInternalServicesError, self).__init__(message, 6)


class APILogicalError(APIError):

    def __init__(self, message):
        super(APILogicalError, self).__init__(message, 8)

