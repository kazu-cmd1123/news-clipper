import crawler
import urllib.parse
import requests
import feedparser

def debug_google_news():
    keyword = "AI"
    encoded_keyword = urllib.parse.quote(keyword)
    url = f"https://news.google.com/rss/search?q={encoded_keyword}&hl=ja&gl=JP&ceid=JP:ja"
    
    print(f"URL: {url}")
    response = requests.get(url, timeout=10)
    print(f"Status: {response.status_code}")
    
    feed = feedparser.parse(response.content)
    print(f"Entries: {len(feed.entries)}")
    
    if len(feed.entries) > 0:
        entry = feed.entries[0]
        print(f"Title: {entry.title}")
        print(f"Published: {getattr(entry, 'published', 'N/A')}")
        print(f"PubDate: {getattr(entry, 'pubDate', 'N/A')}")

if __name__ == "__main__":
    debug_google_news()
