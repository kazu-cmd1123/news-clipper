import crawler
import datetime
import logging

logging.basicConfig(level=logging.INFO)

def test_crawler():
    keyword = "AI"
    print(f"--- Testing Keyword: {keyword} ---")
    
    # Test news without since_dt
    news = crawler.fetch_latest_news(keyword)
    print(f"News found (no since_dt): {len(news)}")
    if news:
        print(f"Latest news: {news[-1]['title']} at {news[-1]['published']}")
        
    # Test news with since_dt = 1 day ago
    since_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    news_since = crawler.fetch_latest_news(keyword, since_dt=since_dt)
    print(f"News found (since 1 day ago): {len(news_since)}")
    
    # Test SNS
    sns = crawler.fetch_sns_posts(keyword)
    print(f"SNS found: {len(sns)}")

if __name__ == "__main__":
    test_crawler()
