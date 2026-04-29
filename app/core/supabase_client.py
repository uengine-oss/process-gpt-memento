"""Supabase 싱글톤 클라이언트 — 모듈 간 공유."""
from __future__ import annotations

import os
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY"),
)
