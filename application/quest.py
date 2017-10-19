#!/usr/bin/env python3

from flask import Flask, jsonify, request

from put_sign_session import PutSignSession
from get_sign_session import GetSignSession
from update_sign_session import UpdateSignSession
from remove_sign_session import RemoveSignSession
from preview_sign_session import PreviewSignSession
from make_messaging_token_session import MakeMessagingTokenSession
from get_matching_session import GetMatchingSession
from search_signs_session import SearchSignsSession
from grant_sign_access_session import GrantSignAccessSession
from map_signs_clusters import MapSignsClustersSession
from add_sign_like_session import AddSignLikeSession
from add_sign_view_session import AddSignViewSession
from check_sign_like_session import CheckSignLikeSession
from check_sign_view_session import CheckSignViewSession
from get_user_likes_session import GetUserLikesSession
from get_user_views_session import GetUserViewsSession
from make_sign_public_link import MakeSignPublicLinkSession

from create_profile_session import CreateProfileSession
from update_profile_session import UpdateProfileSession
from find_profile_session import FindProfileSession
from search_profile_session import SearchProfileSession
from get_profile_session import GetProfileSession
from add_friends_session import AddFriendsSession
from get_friends_session import GetFriendsSession
from get_external_friends_session import GetExternalFriendsSession
from get_user_signs_session import GetUserSignsSession
from remove_user_session import RemoveUserSession

from facebook_auth_session import FacebookAuthSession
from email_auth_stage1_session import EMailAuthStage1Session
from email_auth_stage2_session import EMailAuthStage2Session

from global_context import GlobalContext
import json
import logging


logging.basicConfig(level=logging.INFO, format='#%(process)s\t[%(asctime)s] %(levelname)s: %(module)s: %(message)s')

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
            response.data = json.dumps({'success': False, 'error': {'message': str(ex), 'code': 1}})
            return response

        result = session.execute()
    except Exception as ex:
        response.status_code = 500
        response.data = json.dumps({'success': False, 'error': {'message': str(ex), 'code': 1}})
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

@application.route('/api/sign/update', methods=['POST'])
def update_sign():
    return run_session(request.get_data(), UpdateSignSession)

@application.route('/api/sign/remove', methods=['POST'])
def get_remove():
    return run_session(request.get_data(), RemoveSignSession)

@application.route('/api/sign/preview', methods=['POST'])
def preview_sign():
    return run_session(request.get_data(), PreviewSignSession)

@application.route('/api/sign/search', methods=['POST'])
def search_signs():
    return run_session(request.get_data(), SearchSignsSession)

@application.route('/api/sign/clustering/map', methods=['POST'])
def map_signs_clusters():
    return run_session(request.get_data(), MapSignsClustersSession)

@application.route('/api/sign/access/grant', methods=['POST'])
def grant_sign_access():
    return run_session(request.get_data(), GrantSignAccessSession)

@application.route('/api/sign/likes/add', methods=['POST'])
def add_sign_like():
    return run_session(request.get_data(), AddSignLikeSession)

@application.route('/api/sign/views/add', methods=['POST'])
def add_sign_view():
    return run_session(request.get_data(), AddSignViewSession)

@application.route('/api/sign/likes/find', methods=['POST'])
def check_sign_like():
    return run_session(request.get_data(), CheckSignLikeSession)

@application.route('/api/sign/views/find', methods=['POST'])
def check_sign_view():
    return run_session(request.get_data(), CheckSignViewSession)

@application.route('/api/sign/publiclink', methods=['POST'])
def make_sign_public_link_view():
    return run_session(request.get_data(), MakeSignPublicLinkSession)

@application.route('/api/profile/likes/get', methods=['POST'])
def get_user_likes():
    return run_session(request.get_data(), GetUserLikesSession)

@application.route('/api/profile/views/get', methods=['POST'])
def get_user_views():
    return run_session(request.get_data(), GetUserViewsSession)

@application.route('/api/profile/create', methods=['POST'])
def create_profile():
    return run_session(request.get_data(), CreateProfileSession)

@application.route('/api/profile/update', methods=['POST'])
def update_profile():
    return run_session(request.get_data(), UpdateProfileSession)

@application.route('/api/profile/find', methods=['POST'])
def find_profile():
    return run_session(request.get_data(), FindProfileSession)

@application.route('/api/profile/search', methods=['POST'])
def search_profile():
    return run_session(request.get_data(), SearchProfileSession)

@application.route('/api/profile/get', methods=['POST'])
def get_profile():
    return run_session(request.get_data(), GetProfileSession)

@application.route('/api/profile/friends/external', methods=['POST'])
def get_external_friends():
    return run_session(request.get_data(), GetExternalFriendsSession)

@application.route('/api/profile/friends/get', methods=['POST'])
def get_friends():
    return run_session(request.get_data(), GetFriendsSession)

@application.route('/api/profile/friends/add', methods=['POST'])
def add_friends():
    return run_session(request.get_data(), AddFriendsSession)

@application.route('/api/profile/signs/get', methods=['POST'])
def get_signs():
    return run_session(request.get_data(), GetUserSignsSession)

@application.route('/api/profile/remove', methods=['POST'])
def remove_user():
    return run_session(request.get_data(), RemoveUserSession)

@application.route('/api/messaging/token', methods=['POST'])
def make_messaging_token():
    return run_session(request.get_data(), MakeMessagingTokenSession)

@application.route('/api/matching/get_batch', methods=['POST'])
def get_matching():
    return run_session(request.get_data(), GetMatchingSession)

@application.route('/api/auth/facebook', methods=['POST'])
def facebook_auth():
    return run_session(request.get_data(), FacebookAuthSession)

@application.route('/api/auth/email/stage1', methods=['POST'])
def email_auth_stage1():
    return run_session(request.get_data(), EMailAuthStage1Session)

@application.route('/api/auth/email/stage2', methods=['POST'])
def email_auth_stage2():
    return run_session(request.get_data(), EMailAuthStage2Session)

@application.route("/", methods=['POST', 'GET'])
def hello():
    response = jsonify()
    response.status_code = 200
    response.data = "{ omg }"
    return response

if __name__ == '__main__':
    application.run(debug=True, host="0.0.0.0", port=8080)

