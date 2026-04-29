"""Google OAuth 인증 헬퍼 + Supabase 기반 토큰 검증."""
from __future__ import annotations

import os
from typing import Any, Dict
from urllib.parse import urlencode

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2AuthorizationCodeBearer
from supabase import Client, create_client

load_dotenv()


supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY"),
)

oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl="https://accounts.google.com/o/oauth2/auth",
    tokenUrl="https://oauth2.googleapis.com/token",
)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """토큰으로 Supabase 사용자 조회."""
    try:
        user = supabase.auth.get_user(token)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
            )
        return user.user
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )


def create_auth_error_response(
    supabase: Client,
    tenant_id: str,
    error_message: str = "Authentication required",
):
    """로그인 URL이 포함된 표준 인증 오류 응답을 만든다."""
    try:
        response = (
            supabase.table("tenant_oauth")
            .select("*")
            .eq("tenant_id", tenant_id)
            .single()
            .execute()
        )

        if response.data:
            oauth_settings = response.data
            params = {
                "client_id": oauth_settings["client_id"],
                "redirect_uri": oauth_settings["redirect_uri"],
                "scope": " ".join(
                    [
                        "openid",
                        "https://www.googleapis.com/auth/userinfo.email",
                        "https://www.googleapis.com/auth/userinfo.profile",
                        "https://www.googleapis.com/auth/drive.readonly",
                        "https://www.googleapis.com/auth/drive.file",
                    ]
                ),
                "response_type": "code",
                "access_type": "offline",
                "prompt": "consent",
                "state": tenant_id,
            }
            auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
            return {
                "error": "authentication_required",
                "message": error_message,
                "auth_url": auth_url,
                "tenant_id": tenant_id,
            }
        return {
            "error": "oauth_settings_not_found",
            "message": "OAuth settings not configured for this tenant",
            "tenant_id": tenant_id,
        }
    except Exception as e:
        return {
            "error": "auth_url_generation_failed",
            "message": f"Failed to generate auth URL: {str(e)}",
            "tenant_id": tenant_id,
        }
