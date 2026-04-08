import os
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client, Client
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

def get_db_client() -> Optional[Client]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase credentials not found. DB operations will be skipped.")
        return None
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        return None

def add_keyword(user_id: str, keyword: str) -> bool:
    client = get_db_client()
    if not client:
        return False
    
    # Check if exists
    try:
        existing = client.table("keywords").select("*").eq("user_id", user_id).eq("keyword", keyword).execute()
        if existing.data:
            return True # Already exists
            
        client.table("keywords").insert({
            "user_id": user_id,
            "keyword": keyword
        }).execute()
        return True
    except Exception as e:
        logger.error(f"Error adding keyword: {e}")
        return False

def remove_keyword(user_id: str, keyword: str) -> bool:
    client = get_db_client()
    if not client:
        return False
    try:
        client.table("keywords").delete().eq("user_id", user_id).eq("keyword", keyword).execute()
        return True
    except Exception as e:
        logger.error(f"Error removing keyword: {e}")
        return False

def get_user_keywords(user_id: str) -> List[str]:
    client = get_db_client()
    if not client:
        return []
    try:
        response = client.table("keywords").select("keyword").eq("user_id", user_id).execute()
        return [row["keyword"] for row in response.data]
    except Exception as e:
        logger.error(f"Error fetching keywords: {e}")
        return []

def get_all_users_and_keywords() -> dict:
    """Returns a dict of user_id -> List[{keyword, last_seen_published}]"""
    client = get_db_client()
    if not client:
        return {}
    try:
        response = client.table("keywords").select("user_id, keyword, last_seen_published").execute()
        result = {}
        for row in response.data:
            uid = row["user_id"]
            if uid not in result:
                result[uid] = []
            result[uid].append({
                "keyword": row["keyword"],
                "last_seen_published": row["last_seen_published"]
            })
        return result
    except Exception as e:
        logger.error(f"Error fetching all keywords: {e}")
        return {}

def update_last_seen_published(user_id: str, keyword: str, dt_str: str) -> bool:
    client = get_db_client()
    if not client:
        return False
    try:
        client.table("keywords").update({
            "last_seen_published": dt_str
        }).eq("user_id", user_id).eq("keyword", keyword).execute()
        return True
    except Exception as e:
        logger.error(f"Error updating last_seen_published: {e}")
        return False

def set_delivery_time(user_id: str, time_str: str) -> bool:
    client = get_db_client()
    if not client:
        return False
    try:
        client.table("user_settings").upsert({
            "user_id": user_id,
            "delivery_time": time_str
        }).execute()
        return True
    except Exception as e:
        logger.error(f"Error setting delivery time: {e}")
        return False

def get_delivery_time(user_id: str) -> str:
    client = get_db_client()
    if not client:
        return "07:00"
    try:
        response = client.table("user_settings").select("delivery_time").eq("user_id", user_id).execute()
        if response.data:
            return response.data[0]["delivery_time"]
    except Exception as e:
        logger.error(f"Error fetching delivery time: {e}")
    return "07:00"

def get_all_users_settings() -> dict:
    """Returns a dict of user_id -> delivery_time"""
    client = get_db_client()
    if not client:
        return {}
    try:
        response = client.table("user_settings").select("user_id, delivery_time").execute()
        return {row["user_id"]: row["delivery_time"] for row in response.data}
    except Exception as e:
        logger.error(f"Error fetching all settings: {e}")
        return {}
