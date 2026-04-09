import os
import logging
import urllib.parse
from typing import Dict, Any, List
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()

from fastapi import FastAPI, Request, HTTPException, Header, BackgroundTasks
from dotenv import load_dotenv

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    PushMessageRequest
)
from linebot.v3.webhooks import (
    MessageEvent,
    TextMessageContent
)

import database
import crawler

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="News Clipper & Trend Monitor")

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

import requests

def send_push_message(user_id: str, text: str):
    """ユーザー宛にプッシュメッセージを送信する"""
    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.warning(f"Dry-run Push to {user_id}: {text}")
        return
        
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.push_message_with_http_info(
            PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text=text)]
            )
        )

def send_to_spreadsheet(keyword: str, news_list: List[Dict]):
    """取得したニュースをGoogleスプレッドシート（GAS）に送信する"""
    webhook_url = os.getenv("SPREADSHEET_WEBHOOK_URL")
    if not webhook_url or not news_list:
        return
        
    try:
        # 必要なデータだけを抽出して送信
        payload = {
            "keyword": keyword,
            "news": [
                {
                    "title": n.get("title"),
                    "link": n.get("link"),
                    "published": n.get("published")
                } for n in news_list
            ]
        }
        requests.post(webhook_url, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Error sending to spreadsheet: {e}")

@app.get("/")
def read_root():
    return {"status": "Running", "service": "news-clipper"}

@app.post("/callback")
async def callback(request: Request, x_line_signature: str = Header(None)):
    """Webhook for LINE Messaging API"""
    body = await request.body()
    body_str = body.decode("utf-8")
    
    try:
        handler.handle(body_str, x_line_signature)
    except InvalidSignatureError:
        logger.error("Invalid signature. Please check your channel access token/channel secret.")
        raise HTTPException(status_code=400, detail="Invalid signature")

    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    """Handle text messages from users"""
    text = event.message.text.strip()
    user_id = event.source.user_id
    
    reply_text = ""
    
    if text.startswith("追加"):
        keyword = text.replace("追加", "").strip()
        if keyword:
            success = database.add_keyword(user_id, keyword)
            if success:
                reply_text = f"キーワード「{keyword}」を登録しました。"
            else:
                reply_text = f"キーワード「{keyword}」の登録に失敗しました（既に登録されているか、システムエラーです）。"
        else:
            reply_text = "追加するキーワードを指定してください。\n例: 追加 AI"
            
    elif text.startswith("削除"):
        keyword = text.replace("削除", "").strip()
        if keyword:
            success = database.remove_keyword(user_id, keyword)
            if success:
                reply_text = f"キーワード「{keyword}」を削除しました。"
            else:
                reply_text = f"キーワード「{keyword}」の削除に失敗しました。"
        else:
            reply_text = "削除するキーワードを指定してください。\n例: 削除 AI"
            
    elif text == "一覧":
        keywords = database.get_user_keywords(user_id)
        if keywords:
            reply_text = "登録されているキーワード:\n" + "\n".join([f"- {kw}" for kw in keywords])
        else:
            reply_text = "登録されているキーワードはありません。"

    elif text == "ニュース":
        def fetch_user_news():
            import datetime
            from dateutil import parser as date_parser
            
            user_kw_data = database.get_all_users_and_keywords().get(user_id, [])
            if not user_kw_data:
                send_push_message(user_id, "登録されているキーワードはありません。まずは「追加」でキーワードを登録してください。")
                return
                
            total_message_lines = ["【新着ニュース＆SNS】"]
            
            for item in user_kw_data:
                kw = item["keyword"]
                last_seen_str = item.get("last_seen_published")
                since_dt = None
                if last_seen_str:
                    try:
                        since_dt = date_parser.parse(last_seen_str)
                    except:
                        pass
                
                # 1. ニュース全件取得
                news_list = crawler.fetch_latest_news(kw, since_dt=since_dt)
                
                # 2. SNS投稿取得
                sns_list = crawler.fetch_sns_posts(kw)
                
                if news_list or sns_list:
                    total_message_lines.append(f"\n◆ {kw}")
                    
                    # ニュースの表示とスプレッドシート送信
                    if news_list:
                        total_message_lines.append("  [News]")
                        for news in news_list:
                            total_message_lines.append(f"  ・{news['title']} ({news['published']})\n  {news['link']}")
                        send_to_spreadsheet(kw, news_list)
                        
                        # 最新の日時をDBに保存
                        latest_dt = news_list[-1]["pub_dt"]
                        if latest_dt:
                            database.update_last_seen_published(user_id, kw, latest_dt.isoformat())
                    
                    # SNSの表示とスプレッドシート送信
                    if sns_list:
                        total_message_lines.append("  [SNS/X]")
                        sns_for_sheet = []
                        for sns in sns_list:
                            total_message_lines.append(f"  ・{sns['text']} ({sns['time']})\n  {sns['link']}")
                            sns_for_sheet.append({"title": sns["text"], "link": sns["link"], "published": sns["time"]})
                        send_to_spreadsheet(kw + " (SNS)", sns_for_sheet)

            if len(total_message_lines) > 1:
                # LINEのメッセージサイズ制限に配慮し、長すぎる場合は分割して送る工夫
                full_msg = "\n".join(total_message_lines)
                if len(full_msg) > 4000:
                    send_push_message(user_id, full_msg[:4000] + "\n(長い文章のため省略されました)")
                else:
                    send_push_message(user_id, full_msg)
            else:
                send_push_message(user_id, "前回取得以降の新着情報はありませんでした。")

        import threading
        threading.Thread(target=fetch_user_news).start()
        reply_text = "新着記事とSNS投稿を確認しています。少しお待ちください..."
            
    elif text.startswith("配信時間"):
        time_str = text.replace("配信時間", "").strip()
        time_str = time_str.replace("時", ":00")
        import re
        match = re.search(r"([0-2]?[0-9])", time_str)
        if match:
            hour = match.group(1).zfill(2)
            formatted_time = f"{hour}:00"
            success = database.set_delivery_time(user_id, formatted_time)
            if success:
                reply_text = f"毎日のニュース配信時間を {formatted_time} に設定しました。\n（※システム上、時間はぴったり1時間単位となります）"
            else:
                reply_text = "配信時間の設定に失敗しました。"
        else:
            reply_text = "配信時間は数字（時間）で指定してください。\n例: 配信時間 07:00\n例: 配信時間 8時"
            
    elif text in ["使い方", "ヘルプ"]:
        reply_text = (
            "【🤖 コマンド一覧】\n\n"
            "📰 ニュース機能\n"
            "「追加 [キーワード]」\n"
            "  -> 例: 追加 AI\n"
            "「削除 [キーワード]」\n"
            "  -> 例: 削除 AI\n"
            "「一覧」\n"
            "  -> 登録中のキーワードを確認\n"
            "「ニュース」\n"
            "  -> 今すぐ最新ニュースを取得！\n\n"
            "⚙️ 設定機能\n"
            "「配信時間 [時間]」\n"
            "  -> 例: 配信時間 07:00\n"
            "  ※毎日の自動配信時間を指定できます。"
        )

    else:
        reply_text = "【よく使うコマンド】\n追加 [キーワード]\n削除 [キーワード]\n一覧\nニュース\n\n※すべての機能説明は「ヘルプ」と送信してください。"
        
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

# Cron Endpoints

@app.get("/cron/daily-clip")
def cron_daily_clip(background_tasks: BackgroundTasks):
    """
    毎朝実行される想定。全員のキーワードについてニュースを取得して通知。
    """
    def job():
        import datetime
        from dateutil import parser as date_parser
        jst_tz = datetime.timezone(datetime.timedelta(hours=9))
        current_hour_str = datetime.datetime.now(jst_tz).strftime('%H:00')

        user_kw_all_data = database.get_all_users_and_keywords()
        user_settings = database.get_all_users_settings()
        
        for user_id, keywords_info in user_kw_all_data.items():
            if not keywords_info:
                continue
                
            delivery_time = user_settings.get(user_id, "07:00")
            if delivery_time != current_hour_str:
                continue
            
            total_message_lines = [f"【本日のニュース＆SNS（{delivery_time}配信号）】"]
            
            for item in keywords_info:
                kw = item["keyword"]
                last_seen_str = item.get("last_seen_published")
                since_dt = None
                if last_seen_str:
                    try:
                        since_dt = date_parser.parse(last_seen_str)
                    except:
                        pass
                
                # ニュース取得
                news_list = crawler.fetch_latest_news(kw, since_dt=since_dt)
                # SNS取得
                sns_list = crawler.fetch_sns_posts(kw)
                
                if news_list or sns_list:
                    total_message_lines.append(f"\n◆ {kw}")
                    
                    if news_list:
                        total_message_lines.append("  [News]")
                        for news in news_list:
                            total_message_lines.append(f"  ・{news['title']} ({news['published']})\n  {news['link']}")
                        send_to_spreadsheet(kw, news_list)
                        
                        latest_dt = news_list[-1]["pub_dt"]
                        if latest_dt:
                            database.update_last_seen_published(user_id, kw, latest_dt.isoformat())
                            
                    if sns_list:
                        total_message_lines.append("  [SNS/X]")
                        sns_for_sheet = []
                        for sns in sns_list:
                            total_message_lines.append(f"  ・{sns['text']} ({sns['time']})\n  {sns['link']}")
                            sns_for_sheet.append({"title": sns["text"], "link": sns["link"], "published": sns["time"]})
                        send_to_spreadsheet(kw + " (SNS)", sns_for_sheet)

            if len(total_message_lines) > 1:
                full_msg = "\n".join(total_message_lines)
                if len(full_msg) > 4000:
                    send_push_message(user_id, full_msg[:4000] + "\n(長い文章のため省略されました)")
                else:
                    send_push_message(user_id, full_msg)
                
    background_tasks.add_task(job)
    return {"status": "Daily clip job started"}

@app.get("/cron/trend-monitor")
def cron_trend_monitor(background_tasks: BackgroundTasks):
    """
    数十分ごとに実行される想定。X(Yahoo)での急増を検知。
    """
    def job():
        INFLAMMATION_THRESHOLD = 500  # 例: 閾値。実際の運用で増減させる
        user_keywords = database.get_all_users_and_keywords()
        
        # 同じキーワードを複数人が登録している場合のキャッシュ
        volume_cache = {}
        
        for user_id, keywords_info in user_keywords.items():
            alerts = []
            for item in keywords_info:
                kw = item["keyword"]
                if kw not in volume_cache:
                    vol = crawler.check_trend_volume(kw)
                    volume_cache[kw] = vol
                
                vol = volume_cache[kw]
                if vol > INFLAMMATION_THRESHOLD:
                    encoded_kw = urllib.parse.quote(kw)
                    alerts.append(f"⚠️ 炎上・トレンド検知 ⚠️\nキーワード「{kw}」の言及数が急増しています (約{vol}件)。\n確認リンク: https://search.yahoo.co.jp/realtime/search?p={encoded_kw}")
            
            if alerts:
                send_push_message(user_id, "\n\n".join(alerts))
                
    background_tasks.add_task(job)
    return {"status": "Trend monitor job started"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
