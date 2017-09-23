#!/usr/bin/env python3

from flask import Flask, jsonify, request
from put_sign_session import PutSignSession
from get_sign_session import GetSignSession
from create_profile_session import CreateProfileSession
from update_profile_session import UpdateProfileSession
from find_profile_session import FindProfileSession
from get_profile_session import GetProfileSession
from preview_sign_session import PreviewSignSession
from make_messages_token_session import MakeMessagesTokenSession
from get_matching_session import GetMatchingSession
from facebook_auth_session import FacebookAuthSession
from global_context import GlobalContext
import json
import logging


application = Flask(__name__)

global_context = GlobalContext()
global_context.initialize()

def run_session(data, session_type):
    response = jsonify()
    try:
        session = session_type(global_context)
        try:
            session.parse_query(data)
        except Exception as ex:
            response.status_code = 400
            response.data = json.dumps({'error': {'message': str(ex), 'code': 1}})
            return response

        result = session.execute()
    except Exception as ex:
        response.status_code = 500
        response.data = json.dumps({'error': {'message': str(ex), 'code': 1}})
        return response

    response.status_code = 200
    response.data = result
    return response

@application.route('/api/sign/put', methods=['POST'])
def put_sign():
    return run_session(request.get_data(), PutSignSession)

@application.route('/api/sign/get', methods=['POST'])
def get_sign():
    return run_session(request.get_data(), GetSignSession)

@application.route('/api/sign/preview', methods=['POST'])
def preview_sign():
    return run_session(request.get_data(), PreviewSignSession)

@application.route('/api/profile/create', methods=['POST'])
def create_profile():
    return run_session(request.get_data(), CreateProfileSession)

@application.route('/api/profile/update', methods=['POST'])
def update_profile():
    return run_session(request.get_data(), UpdateProfileSession)

@application.route('/api/profile/find', methods=['POST'])
def find_profile():
    return run_session(request.get_data(), FindProfileSession)

@application.route('/api/profile/get', methods=['POST'])
def get_profile():
    return run_session(request.get_data(), GetProfileSession)

@application.route('/api/message/token', methods=['POST'])
def make_messages_token():
    return run_session(request.get_data(), MakeMessagesTokenSession)

@application.route('/api/matching/get_batch', methods=['POST'])
def get_matching():
    return run_session(request.get_data(), GetMatchingSession)

@application.route('/api/auth/facebook', methods=['POST'])
def facebook_auth():
    return run_session(request.get_data(), FacebookAuthSession)

@application.route("/", methods=['POST', 'GET'])
def hello():
    response = jsonify()
    response.status_code = 200
    response.data = "{ omg }"
    return response

if __name__ == '__main__':
    application.run(debug=True, host="0.0.0.0", port=8080)

