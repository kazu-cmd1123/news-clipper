import urllib.parse
import feedparser
import requests
from bs4 import BeautifulSoup
import re
import logging

logger = logging.getLogger(__name__)

def fetch_latest_news(keyword: str, max_items: int = 3) -> list:
    """
    Google News RSSからキーワードに関する最新記事を取得する
    """
    encoded_keyword = urllib.parse.quote(keyword)
    url = f"https://news.google.com/rss/search?q={encoded_keyword}&hl=ja&gl=JP&ceid=JP:ja"
    
    try:
        feed = feedparser.parse(url)
        results = []
        import datetime
        from email.utils import parsedate_to_datetime
        for entry in feed.entries[:max_items]:
            pub_str = entry.published
            try:
                dt = parsedate_to_datetime(pub_str)
                jst_tz = datetime.timezone(datetime.timedelta(hours=9))
                dt_jst = dt.astimezone(jst_tz)
                pub_str = dt_jst.strftime('%m/%d %H:%M')
            except Exception:
                pass
                
            results.append({
                "title": entry.title,
                "link": entry.link,
                "published": pub_str
            })
        return results
    except Exception as e:
        logger.error(f"Error fetching news for {keyword}: {e}")
        return []

def check_trend_volume(keyword: str) -> int:
    """
    Yahoo!リアルタイム検索から対象キーワードの言及数（概算）を取得する。
    ※スクレイピングに対する対策が強化されている場合は0を返す等、フェールセーフにしています。
    """
    url = "https://search.yahoo.co.jp/realtime/search"
    params = {"p": keyword}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Yahooリアルタイムのページ構造に依存（変更される可能性があります）
        # 例: 「約 1,234 件」のようなテキストを探す
        text = soup.get_text()
        
        # 簡単な正規表現で「約〇〇件」を抽出
        match = re.search(r"約\s*([0-9,]+)\s*件", text)
        if match:
            num_str = match.group(1).replace(",", "")
            return int(num_str)
            
        return 0
    except Exception as e:
        logger.error(f"Error checking trend for {keyword}: {e}")
        return 0
