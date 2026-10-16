import os
import random
import requests
import asyncio
import logging
import yt_dlp
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from config import BOT_TOKEN
from database import init_db, save_user
from celery_app import celery_app  # استيراد إعدادات سيلري

# إعداد التسجيل (Logging)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# إنشاء مجلد التنزيلات
DOWNLOADS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# -----------------------------------------------------------------
# 🔄 دوال إدارة البروكسي (Proxy Pool & Dead Proxies)
# -----------------------------------------------------------------
DEAD_PROXIES = set()

def load_proxies():
    proxies = []
    file_path = os.path.join(os.path.dirname(__file__), 'proxies.txt')
    if not os.path.exists(file_path):
        file_path = os.path.join(os.path.dirname(__file__), 'Webshare 100 proxies (1).txt')

    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(':')
                    if len(parts) == 4:
                        ip, port, user, pwd = parts
                        proxies.append(f"http://{user}:{pwd}@{ip}:{port}")
                    elif len(parts) == 2:
                        ip, port = parts
                        proxies.append(f"http://{ip}:{port}")
    return proxies

def get_proxy():
    proxy_pool = load_proxies()
    if not proxy_pool:
        return None
    available = [p for p in proxy_pool if p not in DEAD_PROXIES]
    if not available:
        DEAD_PROXIES.clear()
        available = proxy_pool
    return random.choice(available)

# -----------------------------------------------------------------
# 🎬 دالة التحميل كمهمة خلفية (Celery Task)
# -----------------------------------------------------------------
@celery_app.task(bind=True)
def download_media_sync(self, url, output_template):
    if 'vt.tiktok.com' in url or 'vm.tiktok.com' in url:
        try:
            res = requests.head(url, allow_redirects=True, timeout=15)
            url = res.url
        except Exception:
            pass

    proxy_pool = load_proxies()
    if not proxy_pool:
        raise Exception("لا توجد بروكسيات متاحة في الملف.")

    max_retries = 15
    for attempt in range(1, max_retries + 1):
        current_proxy = get_proxy()
        if not current_proxy:
            break

        safe_proxy = current_proxy.split('@')[-1] if '@' in current_proxy else current_proxy
        logging.info(f"⚡ محاولة تحميل [{attempt}/{max_retries}] باستخدام البروكسي: {safe_proxy}")

        try:
            ydl_opts = {
                'outtmpl': output_template,
                'noplaylist': True,
                'socket_timeout': 120,  # زيادة المهلة خصيصاً للفيديوهات الطويلة
                'retries': 20,
                'geo_bypass': True,
                'nocheckcertificate': True,
                'proxy': current_proxy,
                'legacy_server_connect': True,
                'source_address': '0.0.0.0',
                'format': 'best[ext=mp4]/best',
                'http_headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept-Language': 'en-US,en;q=0.9',
                },
                'extractor_args': {'youtube': {'player_client': ['android', 'web']}},
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info_dict)

            if filename and os.path.exists(filename):
                return filename
            
            if filename:
                base, _ = os.path.splitext(filename)
                for ext in ['.mp4', '.mkv', '.webm', '.mov']:
                    alt_file = base + ext
                    if os.path.exists(alt_file):
                        return alt_file

        except Exception as e:
            logging.warning(f"❌ فشلت المحاولة {attempt} بالبروكسي {safe_proxy}: {e}")
            if current_proxy:
                DEAD_PROXIES.add(current_proxy)
            continue

    return None

# -----------------------------------------------------------------
# 🤖 أوامر ومعالجة رسائل تيليجرام
# -----------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user.id, user.username, user.first_name, user.last_name)
    
    welcome_text = (
       f"أهلاً بك يا {user.first_name} في بوت VidGo! 🚀\n\n"
        "📎 أرسل رابط المقطع وسأرسله لك مباشرة داخل تيليجرام.\n\n"
        "يدعم حالياً:\n"
        "• TikTok\n"
        "• Instagram\n"
        "• Snapchat\n"
        "• X (Twitter)\n"
        "• YouTube\n"
        "• Facebook\n\n")
    await update.message.reply_text(welcome_text)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    
    if not (url.startswith('http://') or url.startswith('https://')):
        await update.message.reply_text("يرجى إرسال رابط فيديو صحيح يبدأ بـ http أو https 🔗")
        return

    msg = await update.message.reply_text("جاري التحميل ... ⏳")
    file_path = None

    try:
        output_template = os.path.join(DOWNLOADS_DIR, '%(id)s.%(ext)s')
        
        # إرسال المهمة إلى Celery عبر Redis لتنفذ في الخلفية بشكل غير متزامن
        task = download_media_sync.delay(url, output_template)
        
        # انتظار النتيجة بشكل دوري بدون تجميد حلقة أحداث تيليجرام
        loop = asyncio.get_running_loop()
        while not task.ready():
            await asyncio.sleep(1)
        
        file_path = task.get()

        if file_path and os.path.exists(file_path):
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if file_size_mb > 50:
                await msg.edit_text("❌ حجم الفيديو يتجاوز 50 ميجابايت.")
                if os.path.exists(file_path):
                    os.remove(file_path)
                return

            # إرسال الفيديو للمستخدم
            with open(file_path, 'rb') as video_file:
                await update.message.reply_video(
                    video=video_file,
                    caption="تم التحميل بنجاح بواسطة @vidgoo_bot 🚀"
                )
            
            # حذف رسالة الانتظار
            try:
                await msg.delete()
            except Exception:
                pass
            
            # تنظيف الملفات المؤقتة
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                base_name = os.path.splitext(file_path)[0]
                for ext in ['.part', '.ytdl', '.webm', '.mkv']:
                    temp_f = base_name + ext
                    if os.path.exists(temp_f):
                        os.remove(temp_f)
            except Exception:
                pass
            
            return

        else:
            await msg.edit_text("❌ تعذر تحميل الفيديو، تأكد من صحة الرابط.")

    except Exception as e:
        logging.error(f"Error handling request: {e}")
        try:
            await msg.edit_text("❌ حدث خطأ أثناء تحميل الفيديو.")
        except Exception:
            pass
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("⚡ بوت VidGo صاروخي وجاهز مع Celery و Redis...")
    app.run_polling()

if __name__ == '__main__':
    main()