"""main.py / ingest_router.py에서 공유하는 인증 헬퍼."""
from urllib.parse import urlencode
from supabase import Client


def create_auth_error_response(
    supabase: Client,
    tenant_id: str,
    error_message: str = "Authentication required",
):
    """로그인 URL이 포함된 표준 인증 오류 응답을 만든다."""
    try:
        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()

        if response.data:
            oauth_settings = response.data

            params = {
                'client_id': oauth_settings['client_id'],
                'redirect_uri': oauth_settings['redirect_uri'],
                'scope': ' '.join([
                    'openid',
                    'https://www.googleapis.com/auth/userinfo.email',
                    'https://www.googleapis.com/auth/userinfo.profile',
                    'https://www.googleapis.com/auth/drive.readonly',
                    'https://www.googleapis.com/auth/drive.file'
                ]),
                'response_type': 'code',
                'access_type': 'offline',
                'prompt': 'consent',
                'state': tenant_id
            }

            auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"

            return {
                "error": "authentication_required",
                "message": error_message,
                "auth_url": auth_url,
                "tenant_id": tenant_id
            }
        else:
            return {
                "error": "oauth_settings_not_found",
                "message": "OAuth settings not configured for this tenant",
                "tenant_id": tenant_id
            }

    except Exception as e:
        return {
            "error": "auth_url_generation_failed",
            "message": f"Failed to generate auth URL: {str(e)}",
            "tenant_id": tenant_id
        }
