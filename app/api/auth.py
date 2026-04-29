"""Google OAuth 라우터: /auth/google/url, /auth/google/status, /auth/google/save-token, /auth/google/callback."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, HTTPException

from app.core.supabase_client import supabase
from app.schemas import GoogleOAuthCallbackRequest, GoogleTokenRequest
from urllib.parse import urlencode

router = APIRouter()


@router.get("/auth/google/url")
async def get_google_auth_url(tenant_id: str):
    """테넌트의 Google OAuth 인가 URL 발급."""
    try:
        response = (
            supabase.table("tenant_oauth")
            .select("*")
            .eq("tenant_id", tenant_id)
            .single()
            .execute()
        )
        if not response.data:
            raise HTTPException(status_code=404, detail="OAuth settings not found for tenant")

        oauth_settings = response.data
        params = {
            "client_id": oauth_settings["client_id"],
            "redirect_uri": oauth_settings["redirect_uri"],
            "scope": " ".join([
                "openid",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/userinfo.profile",
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/drive.file",
            ]),
            "response_type": "code",
            "access_type": "offline",
            "prompt": "consent",
            "state": tenant_id,
        }
        auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
        return {"auth_url": auth_url, "state": tenant_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/auth/google/status")
async def get_google_auth_status(tenant_id: str):
    """테넌트가 유효한 Google OAuth 토큰을 보유했는지 확인."""
    try:
        response = (
            supabase.table("tenant_oauth")
            .select("google_credentials, google_credentials_updated_at")
            .eq("tenant_id", tenant_id)
            .single()
            .execute()
        )

        if not response.data or not response.data.get("google_credentials"):
            return {"authenticated": False, "message": "No Google credentials found"}

        if isinstance(response.data["google_credentials"], str):
            token_data = json.loads(response.data["google_credentials"])
        else:
            token_data = response.data["google_credentials"]

        if token_data.get("expiry"):
            expiry = datetime.fromisoformat(token_data["expiry"])
            if datetime.utcnow() > expiry:
                return {"authenticated": False, "message": "Token expired"}

        return {
            "authenticated": True,
            "tenant_id": tenant_id,
            "expires_at": token_data.get("expiry"),
            "updated_at": response.data.get("google_credentials_updated_at"),
        }
    except Exception as e:
        return {"authenticated": False, "message": str(e)}


@router.post("/auth/google/save-token")
async def save_google_token(request: GoogleTokenRequest):
    """Google OAuth 토큰을 테넌트의 google_credentials 컬럼에 저장."""
    try:
        tenant_check = (
            supabase.table("tenant_oauth")
            .select("tenant_id")
            .eq("tenant_id", request.tenant_id)
            .single()
            .execute()
        )

        if not tenant_check.data:
            raise HTTPException(status_code=404, detail=f"Tenant OAuth settings not found for tenant {request.tenant_id}")

        token_data = {
            "access_token": request.access_token,
            "refresh_token": request.refresh_token,
            "token_type": request.token_type,
            "expires_in": request.expires_in,
            "scopes": request.scopes or [
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/drive.file",
            ],
        }

        if request.expires_in:
            expiry = datetime.now(timezone.utc) + timedelta(seconds=request.expires_in)
            token_data["expiry"] = expiry.isoformat()

        response = (
            supabase.table("tenant_oauth")
            .update({
                "google_credentials": json.dumps(token_data),
                "google_credentials_updated_at": datetime.now(timezone.utc).isoformat(),
            })
            .eq("tenant_id", request.tenant_id)
            .execute()
        )

        if not response.data:
            raise HTTPException(status_code=500, detail="Failed to update tenant credentials")

        return {"message": "Google token saved successfully", "tenant_id": request.tenant_id}

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to save Google token: {str(e)}")


@router.post("/auth/google/callback")
async def google_oauth_callback(request: GoogleOAuthCallbackRequest):
    """Google OAuth 콜백 처리 — 인가 코드를 토큰으로 교환."""
    try:
        tenant_id = request.state
        response = (
            supabase.table("tenant_oauth")
            .select("*")
            .eq("tenant_id", tenant_id)
            .single()
            .execute()
        )
        if not response.data:
            raise HTTPException(status_code=404, detail="OAuth settings not found for tenant")

        oauth_settings = response.data
        token_url = "https://oauth2.googleapis.com/token"
        token_data = {
            "client_id": oauth_settings["client_id"],
            "client_secret": oauth_settings["client_secret"],
            "code": request.code,
            "grant_type": "authorization_code",
            "redirect_uri": oauth_settings["redirect_uri"],
        }

        async with httpx.AsyncClient() as client:
            token_response = await client.post(token_url, data=token_data)
            if token_response.status_code != 200:
                raise HTTPException(status_code=400, detail=f"Failed to exchange code for token: {token_response.text}")
            token_info = token_response.json()
            if "access_token" not in token_info:
                raise HTTPException(status_code=400, detail=f"Token response missing access_token: {token_info}")

        token_request = GoogleTokenRequest(
            tenant_id=tenant_id,
            access_token=token_info["access_token"],
            refresh_token=token_info.get("refresh_token"),
            expires_in=token_info.get("expires_in"),
            token_type=token_info.get("token_type", "Bearer"),
            scopes=request.scope.split(" ") if request.scope else None,
        )

        await save_google_token(token_request)

        return {"message": "Google OAuth completed successfully", "tenant_id": tenant_id, "token_saved": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
