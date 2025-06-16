from typing import Dict, Any, Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2AuthorizationCodeBearer
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import json

load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# OAuth2 configuration
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile'
]

oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl="https://accounts.google.com/o/oauth2/auth",
    tokenUrl="https://oauth2.googleapis.com/token"
)

async def get_tenant_oauth(tenant_id: str) -> Dict[str, Any]:
    """Get OAuth settings for a specific tenant"""
    try:
        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .eq("provider", "google") \
            .single() \
            .execute()
        
        if not response.data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="OAuth settings not found for tenant"
            )
            
        return response.data
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching OAuth settings: {str(e)}"
        )

async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """Get current user from token"""
    try:
        # Verify token and get user info from Supabase
        user = supabase.auth.get_user(token)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials"
            )
        return user.user
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )

async def get_google_drive_service(current_user: Dict[str, Any]):
    """Get Google Drive service for the current user"""
    try:
        # Get tenant OAuth settings
        oauth_settings = await get_tenant_oauth(current_user.app_metadata['tenant_id'])
        
        # Check if user has valid credentials
        user_data = supabase.table("users") \
            .select("google_credentials") \
            .eq("id", current_user.id) \
            .eq("tenant_id", current_user.app_metadata['tenant_id']) \
            .single() \
            .execute()
            
        if user_data.data and user_data.data.get("google_credentials"):
            try:
                # Try to use existing credentials
                credentials = Credentials.from_authorized_user_info(
                    json.loads(user_data.data["google_credentials"]),
                    SCOPES
                )
                
                # Check if credentials are valid
                if credentials.valid:
                    return build("drive", "v3", credentials=credentials)
                    
                # If credentials are expired but refreshable, refresh them
                if credentials.expired and credentials.refresh_token:
                    credentials.refresh(None)
                    # Update stored credentials
                    supabase.table("users") \
                        .update({
                            "google_credentials": json.dumps(credentials.to_json()),
                            "google_credentials_updated_at": "now()"
                        }) \
                        .eq("id", current_user.id) \
                        .eq("tenant_id", current_user.app_metadata['tenant_id']) \
                        .execute()
                    return build("drive", "v3", credentials=credentials)
            except Exception:
                # If there's any error with existing credentials, continue to get new ones
                pass
        
        # If no valid credentials, start OAuth flow
        flow = Flow.from_client_config(
            {
                "web": {
                    "client_id": oauth_settings["client_id"],
                    "client_secret": oauth_settings["client_secret"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uri": oauth_settings["redirect_uri"]
                }
            },
            scopes=SCOPES
        )
        
        # Get the authorization URL
        auth_url, _ = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true'
        )
        
        # Return the authorization URL for the client to redirect to
        return {"auth_url": auth_url}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting Google Drive service: {str(e)}"
        )

async def handle_oauth_callback(code: str, current_user: Dict[str, Any]):
    """Handle OAuth callback and get access token"""
    try:
        # Get tenant OAuth settings
        oauth_settings = await get_tenant_oauth(current_user.app_metadata['tenant_id'])
        
        # Create OAuth2 flow
        flow = Flow.from_client_config(
            {
                "web": {
                    "client_id": oauth_settings["client_id"],
                    "client_secret": oauth_settings["client_secret"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uri": oauth_settings["redirect_uri"]
                }
            },
            scopes=SCOPES
        )
        
        # Exchange authorization code for credentials
        flow.fetch_token(code=code)
        credentials = flow.credentials
        
        # Store credentials in user record
        supabase.table("users").update({
            "google_credentials": json.dumps(credentials.to_json()),
            "google_credentials_updated_at": "now()"
        }).eq("id", current_user.id).eq("tenant_id", current_user.app_metadata['tenant_id']).execute()
        
        # Build and return Drive service
        return build("drive", "v3", credentials=credentials)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error handling OAuth callback: {str(e)}"
        )