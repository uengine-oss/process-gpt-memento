"""Supabase 싱글톤 클라이언트 — 모듈 간 공유."""
from __future__ import annotations

import os
from supabase import Client, create_client

from app.core.env_loader import load_project_dotenv

load_project_dotenv()

supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY"),
)
