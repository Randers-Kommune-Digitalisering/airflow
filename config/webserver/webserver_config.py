import jwt
import logging
import os
import requests

from flask import redirect, session
from flask_appbuilder import expose
from flask_appbuilder.security.manager import AUTH_OAUTH
from flask_appbuilder.security.views import AuthOAuthView

from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride


log = logging.getLogger(__name__)

CSRF_ENABLED = True
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION_ROLE = "Public"
PERMANENT_SESSION_LIFETIME = 43200

AUTH_ROLES_MAPPING = {
    "Admin": ["Admin"],
    "Op": ["Op"],
    "User": ["User"],
    "Viewer": ["Viewer"],
    "Public": ["Public"],
    "Nexus": ["Nexus"],
    "Affaldsterminal": ["Affaldsterminal"],
    "Vognpark": ["Vognpark"],
}

PROVIDER_NAME = "keycloak"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
AIRFLOW__API__BASE_URL = os.getenv("AIRFLOW__API__BASE_URL")
OIDC_ISSUER = os.getenv("OIDC_ISSUER")
OIDC_BASE_URL = f"{OIDC_ISSUER}/protocol/openid-connect"
OIDC_TOKEN_URL = f"{OIDC_BASE_URL}/token"
OIDC_AUTH_URL = f"{OIDC_BASE_URL}/auth"
OIDC_METADATA_URL = f"{OIDC_ISSUER}/.well-known/openid-configuration"
OAUTH_PROVIDERS = [
    {
        "name": PROVIDER_NAME,
        "token_key": "access_token",
        "icon": "fa-key",
        "remote_app": {
            "api_base_url": OIDC_BASE_URL,
            "access_token_url": OIDC_TOKEN_URL,
            "authorize_url": OIDC_AUTH_URL,
            "server_metadata_url": OIDC_METADATA_URL,
            "request_token_url": None,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "client_kwargs": {
                "scope": "email profile",
            },
        },
    }
]


def get_jwks_public_key_for_kid(kid):
    metadata = requests.get(OIDC_METADATA_URL).json()
    jwks_uri = metadata["jwks_uri"]
    jwks = requests.get(jwks_uri).json()
    for key_data in jwks["keys"]:
        if key_data.get("kid") == kid:
            from jwt.algorithms import RSAAlgorithm
            return RSAAlgorithm.from_jwk(key_data)
    return None


class CustomOAuthView(AuthOAuthView):
    @expose("/logout/", methods=["GET", "POST"])
    def logout(self):
        session.clear()
        return redirect(
            f"{OIDC_ISSUER}/protocol/openid-connect/logout?post_logout_redirect_uri={AIRFLOW__API__BASE_URL}&client_id={CLIENT_ID}"
        )


class CustomSecurityManager(FabAirflowSecurityManagerOverride):
    authoauthview = CustomOAuthView

    def get_oauth_user_info(self, provider, response):
        if provider == "keycloak":
            token = response["access_token"]
            try:
                unverified_header = jwt.get_unverified_header(token)
                kid = unverified_header.get("kid")
                public_key = get_jwks_public_key_for_kid(kid)
                if not public_key:
                    log.error(f"No matching JWKS key found for kid: {kid}")
                    return {}

                me = jwt.decode(token, public_key, algorithms=["RS256", "HS256"], options={"verify_aud": False})
            except Exception as e:
                log.error(f"JWT decode error: {e}")
                return {}

            aud = me.get("aud")
            if isinstance(aud, str):
                aud_list = [aud]
            else:
                aud_list = aud if isinstance(aud, list) else []
            if CLIENT_ID not in aud_list:
                log.error(f"Audience mismatch: {aud_list} does not contain {CLIENT_ID}")
                return {}

            resource_access = me.get("resource_access", {})
            log.info(f"Token resource_access: {resource_access}")
            log.info(f"CLIENT_ID for role extraction: {CLIENT_ID}")
            groups = resource_access.get(CLIENT_ID, {}).get("roles", [])
            log.info(f"Extracted groups: {groups}")
            if not groups:
                groups = ["Viewer"]
            userinfo = {
                "username": me.get("preferred_username"),
                "email": me.get("email"),
                "first_name": me.get("given_name", "Unknown"),
                "last_name": me.get("family_name", "Unknown"),
                "role_keys": groups,
            }
            log.info(f"user info: {userinfo}")
            return userinfo
        else:
            return {}


SECURITY_MANAGER_CLASS = CustomSecurityManager
