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

def send_to_spreadsheet(user_id: str, keyword: str, news_list: List[Dict]):
    """
    取得したニュースをユーザー個別のGoogleスプレッドシート（GAS）に送信する。
    URLが設定されていない場合は何もしない。
    """
    if not news_list:
        return
        
    # ユーザー個別のURLを取得
    webhook_url = database.get_spreadsheet_url(user_id)
    
    # 未設定の場合は環境変数をフォールバックとして使用（管理者のデフォルト設定用）
    if not webhook_url:
        webhook_url = os.getenv("SPREADSHEET_WEBHOOK_URL")
        
    if not webhook_url:
        return
        
    try:
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
        logger.error(f"Error sending to spreadsheet for user {user_id}: {e}")


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
                
            any_news_found = False
            
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
                    any_news_found = True
                    
                    # ニュースの表示と送信
                    if news_list:
                        news_msg_lines = [f"📰 【ニュース】{kw}"]
                        for news in news_list:
                            news_msg_lines.append(f"・{news['title']} ({news['published']})\n{news['link']}")
                        
                        send_to_spreadsheet(user_id, kw, news_list)
                        
                        # 最新の日時をDBに保存
                        latest_dt = news_list[-1]["pub_dt"]
                        if latest_dt:
                            database.update_last_seen_published(user_id, kw, latest_dt.isoformat())
                        
                        # LINE送信
                        news_msg = "\n\n".join(news_msg_lines)
                        if len(news_msg) > 4000:
                            send_push_message(user_id, news_msg[:4000] + "\n(文字数制限のため一部省略)")
                        else:
                            send_push_message(user_id, news_msg)
                    
                    # SNSの表示と送信
                    if sns_list:
                        sns_msg_lines = [f"🐦 【SNS/X】{kw}"]
                        sns_for_sheet = []
                        for sns in sns_list:
                            sns_msg_lines.append(f"・{sns['text']} ({sns['time']})\n{sns['link']}")
                            sns_for_sheet.append({"title": sns["text"], "link": sns["link"], "published": sns["time"]})
                        
                        send_to_spreadsheet(user_id, kw + " (SNS)", sns_for_sheet)

                        # LINE送信
                        sns_msg = "\n\n".join(sns_msg_lines)
                        if len(sns_msg) > 4000:
                            send_push_message(user_id, sns_msg[:4000] + "\n(文字数制限のため一部省略)")
                        else:
                            send_push_message(user_id, sns_msg)

            if not any_news_found:
                send_push_message(user_id, "前回取得以降の新着情報はありませんでした。")

        import threading
        threading.Thread(target=fetch_user_news).start()
        reply_text = "新着記事とSNS投稿を確認しています。少しお待ちください..."
            
    elif text.startswith("配信時間"):
        import re
        subcmd_str = text.replace("配信時間", "").strip()

        def parse_time(s: str):
            """文字列から HH:00 形式を抽出する"""
            s = s.replace("時", ":00")
            m = re.search(r"([0-2]?[0-9])(?::00)?", s)
            if m:
                return f"{m.group(1).zfill(2)}:00"
            return None

        if subcmd_str == "一覧" or subcmd_str == "":
            # 現在の設定を表示
            times = database.get_delivery_times(user_id)
            times_str = "\n".join([f"  {i+1}. {t}" for i, t in enumerate(times)])
            reply_text = (
                f"現在の配信時間（最大4つ）:\n{times_str}\n\n"
                "追加: 配信時間 追加 [時刻]\n"
                "削除: 配信時間 削除 [時刻]\n"
                "例: 配信時間 追加 12:00"
            )

        elif subcmd_str.startswith("追加"):
            time_part = subcmd_str.replace("追加", "").strip()
            formatted = parse_time(time_part)
            if formatted:
                ok, msg = database.add_delivery_time(user_id, formatted)
                if ok:
                    times = database.get_delivery_times(user_id)
                    reply_text = (
                        f"配信時間 {formatted} を追加しました。\n"
                        f"現在の設定: {', '.join(times)}"
                    )
                else:
                    reply_text = f"追加できませんでした。\n{msg}"
            else:
                reply_text = "時刻の形式が正しくありません。\n例: 配信時間 追加 07:00\n例: 配信時間 追加 8時"

        elif subcmd_str.startswith("削除"):
            time_part = subcmd_str.replace("削除", "").strip()
            formatted = parse_time(time_part)
            if formatted:
                ok, msg = database.remove_delivery_time(user_id, formatted)
                if ok:
                    times = database.get_delivery_times(user_id)
                    reply_text = (
                        f"配信時間 {formatted} を削除しました。\n"
                        f"現在の設定: {', '.join(times)}"
                    )
                else:
                    reply_text = f"削除できませんでした。\n{msg}"
            else:
                reply_text = "時刻の形式が正しくありません。\n例: 配信時間 削除 07:00"

        else:
            # 後方互換: "配信時間 07:00" → 追加として扱う
            formatted = parse_time(subcmd_str)
            if formatted:
                ok, msg = database.add_delivery_time(user_id, formatted)
                if ok:
                    times = database.get_delivery_times(user_id)
                    reply_text = (
                        f"配信時間 {formatted} を追加しました。\n"
                        f"現在の設定: {', '.join(times)}"
                    )
                else:
                    reply_text = f"追加できませんでした。\n{msg}"
            else:
                reply_text = (
                    "配信時間コマンドの使い方:\n"
                    "・配信時間 一覧  → 現在の設定を確認\n"
                    "・配信時間 追加 [時刻]  → 追加（最大4つ）\n"
                    "・配信時間 削除 [時刻]  → 削除\n"
                    "例: 配信時間 追加 07:00"
                )
            
    elif text.startswith("連携"):
        import re
        if text.startswith("連携手順"):
            reply_text = (
                "【📊 スプレッドシート連携手順】\n\n"
                "1. Googleスプレッドシートを新規作成します。\n"
                "2. 「拡張機能」>「Apps Script」を開きます。\n"
                "3. 元のコードをすべて消して、以下のガイドページにある「GASコード」を貼り付けて保存します。\n"
                "   👉 [連携用GASコードを表示]\n"
                "4. 右上の「デプロイ」>「新しいデプロイ」を選択。\n"
                "5. 種類を「ウェブアプリ」にし、アクセスできるユーザーを「全員」に設定してデプロイします。\n"
                "6. 発行された「ウェブアプリのURL」をコピーして、この画面で以下のように送信してください。\n\n"
                "「連携 https://script.google.com/...」\n\n"
                "※これで、あなた専用のシートに自動でニュースが記録されるようになります！"
            )
        elif text == "連携解除":
            database.set_spreadsheet_url(user_id, "")
            reply_text = "スプレッドシート連携を解除しました。"
        else:
            url_match = re.search(r"(https://script\.google\.com/[^\s]+)", text)
            if url_match:
                url = url_match.group(1)
                success = database.set_spreadsheet_url(user_id, url)
                if success:
                    reply_text = "スプレッドシート連携を設定しました！今後、新着ニュースがあなたのシートに自動転送されます。"
                else:
                    reply_text = "連携設定中にエラーが発生しました。もう一度お試しください。"
            else:
                reply_text = (
                    "連携するURLが正しくありません。\n"
                    "例: 連携 https://script.google.com/...\n\n"
                    "手順がわからない場合は「連携手順」と送信してください。"
                )

    elif text in ["使い方", "ヘルプ"]:
        reply_text = (
            "【🤖 コマンド一覧】\n\n"
            "📰 ニュース機能\n"
            "「追加 [キーワード]」\n"
            "「削除 [キーワード]」\n"
            "「一覧」\n"
            "「ニュース」（今すぐ取得）\n\n"
            "⚙️ 設定機能\n"
            "「配信時間 一覧/追加/削除」\n"
            "  -> 毎日決まった時間に自動配信します\n\n"
            "📊 スプレッドシート連携\n"
            "「連携手順」\n"
            "  -> 設定方法のガイドを表示します\n"
            "「連携 [GASのURL]」\n"
            "  -> 自分専用のシートに記録を開始します\n"
            "「連携解除」\n"
            "  -> 連携をストップします"
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
        try:
            import datetime
            from dateutil import parser as date_parser
            jst_tz = datetime.timezone(datetime.timedelta(hours=9))
            current_hour_str = datetime.datetime.now(jst_tz).strftime('%H:00')

            user_kw_all_data = database.get_all_users_and_keywords()
            user_settings = database.get_all_users_settings()
            
            for user_id, keywords_info in user_kw_all_data.items():
                if not keywords_info:
                    continue
                    
                settings = user_settings.get(user_id, {"delivery_times": ["07:00"], "spreadsheet_url": None})
                delivery_times = settings.get("delivery_times", ["07:00"])
                user_spreadsheet_url = settings.get("spreadsheet_url")

                if current_hour_str not in delivery_times:
                    continue
                
                logger.info(f"Starting scheduled delivery for user {user_id} at {current_hour_str}")
                
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
                        # ニュースの送信
                        if news_list:
                            news_msg_lines = [f"📰 【本日のニュース: {kw}】"]
                            for news in news_list:
                                news_msg_lines.append(f"・{news['title']} ({news['published']})\n{news['link']}")
                            
                            # 個別スプレッドシート送信
                            send_to_spreadsheet(user_id, kw, news_list)
                            
                            latest_dt = news_list[-1]["pub_dt"]
                            if latest_dt:
                                database.update_last_seen_published(user_id, kw, latest_dt.isoformat())
                            
                            # LINE送信
                            news_msg = "\n\n".join(news_msg_lines)
                            if len(news_msg) > 4000:
                                send_push_message(user_id, news_msg[:4000] + "\n(文字数制限のため一部省略)")
                            else:
                                send_push_message(user_id, news_msg)
                                
                        # SNSの送信
                        if sns_list:
                            sns_msg_lines = [f"🐦 【本日のSNS/X: {kw}】"]
                            sns_for_sheet = []
                            for sns in sns_list:
                                sns_msg_lines.append(f"・{sns['text']} ({sns['time']})\n{sns['link']}")
                                sns_for_sheet.append({"title": sns["text"], "link": sns["link"], "published": sns["time"]})
                            
                            # 個別スプレッドシート送信 (SNS)
                            send_to_spreadsheet(user_id, kw + " (SNS)", sns_for_sheet)

                            # LINE送信
                            sns_msg = "\n\n".join(sns_msg_lines)
                            if len(sns_msg) > 4000:
                                send_push_message(user_id, sns_msg[:4000] + "\n(文字数制限のため一部省略)")
                            else:
                                send_push_message(user_id, sns_msg)
        except Exception as e:
            logger.error(f"Fatal error in daily-clip background job: {e}")
                
    background_tasks.add_task(job)
    return {"status": "Daily clip job started"}

@app.get("/cron/trend-monitor")
def cron_trend_monitor(background_tasks: BackgroundTasks):
    """
    数十分ごとに実行される想定。X(Yahoo)での急増を検知。
    """
    def job():
        try:
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
        except Exception as e:
            logger.error(f"Fatal error in trend-monitor background job: {e}")
                
    background_tasks.add_task(job)
    return {"status": "Trend monitor job started"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
