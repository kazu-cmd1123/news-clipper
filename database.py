import os
import json
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

# ---------------------------------------------------------------------------
# 複数配信時間（最大4つ）対応
# user_settings テーブルの delivery_time カラムに JSON 配列文字列を保存する
# 例: '["07:00", "12:00"]'
# ---------------------------------------------------------------------------

MAX_DELIVERY_TIMES = 4

def _parse_times(raw: Optional[str]) -> List[str]:
    """DBから取得した生文字列をリストに変換する（旧フォーマット互換）"""
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    # 旧フォーマット（単一文字列 "07:00"）の後方互換
    return [raw] if raw else []


def set_delivery_times(user_id: str, times: List[str]) -> bool:
    """配信時間リスト（最大4つ）を保存する"""
    client = get_db_client()
    if not client:
        return False
    try:
        client.table("user_settings").upsert({
            "user_id": user_id,
            "delivery_time": json.dumps(times)
        }).execute()
        return True
    except Exception as e:
        logger.error(f"Error setting delivery times: {e}")
        return False


def get_delivery_times(user_id: str) -> List[str]:
    """ユーザーの配信時間リストを取得する。未設定なら ["07:00"] を返す"""
    client = get_db_client()
    if not client:
        return ["07:00"]
    try:
        response = client.table("user_settings").select("delivery_time").eq("user_id", user_id).execute()
        if response.data:
            return _parse_times(response.data[0].get("delivery_time")) or ["07:00"]
    except Exception as e:
        logger.error(f"Error fetching delivery times: {e}")
    return ["07:00"]


def set_spreadsheet_url(user_id: str, url: str) -> bool:
    """ユーザー専用のスプレッドシートWebhook URLを保存する"""
    client = get_db_client()
    if not client:
        return False
    try:
        client.table("user_settings").upsert({
            "user_id": user_id,
            "spreadsheet_url": url
        }).execute()
        return True
    except Exception as e:
        logger.error(f"Error setting spreadsheet url: {e}")
        return False


def get_spreadsheet_url(user_id: str) -> Optional[str]:
    """ユーザーのスプレッドシートURLを取得する"""
    client = get_db_client()
    if not client:
        return None
    try:
        response = client.table("user_settings").select("spreadsheet_url").eq("user_id", user_id).execute()
        if response.data:
            return response.data[0].get("spreadsheet_url")
    except Exception as e:
        logger.error(f"Error fetching spreadsheet url: {e}")
    return None


def add_delivery_time(user_id: str, time_str: str) -> tuple[bool, str]:
    """
    配信時間を追加する。
    Returns: (success: bool, message: str)
    """
    current = get_delivery_times(user_id)
    # デフォルト値のみの場合は除外して追加判定
    if current == ["07:00"] and time_str != "07:00":
        current = []
    if time_str in current:
        return False, f"{time_str} は既に設定済みです。"
    if len(current) >= MAX_DELIVERY_TIMES:
        return False, f"配信時間は最大{MAX_DELIVERY_TIMES}つまで設定できます。現在: {', '.join(current)}"
    current.append(time_str)
    current.sort()
    ok = set_delivery_times(user_id, current)
    return ok, ""


def remove_delivery_time(user_id: str, time_str: str) -> tuple[bool, str]:
    """
    配信時間を削除する。
    Returns: (success: bool, message: str)
    """
    current = get_delivery_times(user_id)
    if time_str not in current:
        return False, f"{time_str} は設定されていません。現在: {', '.join(current)}"
    current.remove(time_str)
    if not current:
        return False, "少なくとも1つの配信時間が必要です。"
    ok = set_delivery_times(user_id, current)
    return ok, ""


def get_all_users_settings() -> dict:
    """Returns a dict of user_id -> {delivery_times: List[str], spreadsheet_url: str}"""
    client = get_db_client()
    if not client:
        return {}
    try:
        response = client.table("user_settings").select("user_id, delivery_time, spreadsheet_url").execute()
        return {
            row["user_id"]: {
                "delivery_times": _parse_times(row.get("delivery_time")) or ["07:00"],
                "spreadsheet_url": row.get("spreadsheet_url")
            }
            for row in response.data
        }
    except Exception as e:
        logger.error(f"Error fetching all settings: {e}")
        return {}
