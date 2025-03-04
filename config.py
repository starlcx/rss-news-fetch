import os

# rss_processor.py
RSS_SOURCES = {
    "Yahoo Finance": "http://finance.yahoo.com/rss/topstories",
    "Business Insider": "https://www.businessinsider.com/rss",
    "CNBC": "https://www.cnbc.com/id/100003114/device/rss/rss.html"
}
DATA_DIR = "data"
ARCHIVE_FILE = os.path.join(DATA_DIR, "news_archive.pkl")


# ======================
# DeepSeek API 配置
# ======================
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")  # 从环境变量获取密钥
API_ENDPOINT = "https://api.deepseek.com/v1/summarize"
HEADERS = {
    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
    "Content-Type": "application/json"
}


