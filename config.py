import os
from dotenv import load_dotenv
from openai import OpenAI

# 加载 .env 文件
load_dotenv()

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
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
client = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com/v1",  # 使用兼容OpenAI的端点
)

