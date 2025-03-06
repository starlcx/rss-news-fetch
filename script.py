import re
import os
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
from openai import OpenAI
from typing import Optional

from config import DEEPSEEK_API_KEY, client

# ======================
# DeepSeek API 配置（更新版）
# ======================

def generate_summary(content: str) -> Optional[str]:
    """
    使用DeepSeek API生成新闻摘要
    参数：
        content (str): 新闻正文内容
    返回：
        str: 生成的摘要文本（项目符号列表形式），失败时返回None
    """
    # 输入验证
    if not content or len(content) < 100:
        print("内容过短，跳过摘要生成")
        return None
    try:
        # 构造API请求
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system",
                    "content": """你是一个专业的新闻摘要生成器。请严格按照以下要求处理：
1. 使用英文生成摘要
2. 使用项目符号列表格式（bullet points）
3. 保持摘要简洁（3-5个要点）
4. 只包含关键事实信息"""
                },
                {
                    "role": "user",
                    "content": f"请为以下新闻生成摘要：\n\n{content}"
                }
            ],
            temperature=0.3,  # 控制输出随机性（0-2）
            max_tokens=2000,   # 控制输出长度
            timeout=60        # 请求超时时间
        )
        # 解析响应
        if response.choices and response.choices[0].message.content:
            summary = response.choices[0].message.content.strip()
            return summary
        print("未收到有效响应内容")
        return None
    except Exception as e:
        # 处理不同类型的异常
        error_type = type(e).__name__
        if "APIConnectionError" in error_type:
            print(f"API连接失败: {str(e)}")
        elif "APIError" in error_type:
            print(f"API服务错误: {str(e)}")
        elif "Timeout" in error_type:
            print("API请求超时")
        else:
            print(f"未知错误: {str(e)}")
        return None



# ======================
# 增强型内容提取流程
# ======================
def enhanced_content_extraction(row) -> pd.Series:
    """
    集成内容提取和摘要生成的复合处理函数
    返回包含content和summary的Series
    """
    # 第一步：提取正文内容
    url = row['link']
    parser = url_matcher(url)
    content = parser(url) if parser else None
    # 第二步：生成摘要
    summary = None
    """
    if content:
        summary = generate_summary(content)
        # 添加安全间隔防止速率限制
        time.sleep(0.5)  # 根据API限制调整
    """
    return pd.Series({
        'content': content,
        'summary': summary,
        'if_summ': pd.notna(summary)  # 自动设置状态标记
    })

# ======================
# 主处理流程
# ======================
def process_links():
    try:
        archive_df = pd.read_pickle("data/news_archive.pkl")
    except FileNotFoundError:
        print("Error: news_archive.pkl file not found")
        return pd.DataFrame()
    # 筛选需要处理的记录
    now = pd.Timestamp.now(tz='UTC')
    time_threshold = now - pd.Timedelta(hours=24)
    mask = (
        (archive_df['eastern_time'] >= time_threshold) &
        (~archive_df['if_summ'])
    )
    to_process = archive_df.loc[mask].copy()
    if to_process.empty:
        print("没有需要处理的新记录")
        return pd.DataFrame()
    # 批量处理（使用progress_apply需要tqdm）
    result_cols = to_process.apply(enhanced_content_extraction, axis=1)
    # 合并结果
    to_process.update(result_cols)
    # 更新主数据
    updated_mask = to_process['if_summ']
    archive_df.loc[updated_mask.index, ['content', 'summary', 'if_summ']] = to_process[['content', 'summary', 'if_summ']]
    # 保存更新
    archive_df.to_pickle("data/news_archive.pkl")
    return archive_df.loc[updated_mask.index, ['link', 'summary', 'utc_time']]


# ======================
# 辅助函数
# ======================
def url_matcher(url):
    """优化后的URL匹配逻辑"""
    if url.startswith(("https://finance.yahoo.com/news/",
                     "http://finance.yahoo.com/news/")):
        return extract_yahoo_content
    elif "cnbc.com/202" in url:  # 更灵活的CNBC地址匹配
        return extract_cnbc_content
    elif "businessinsider.com" in url:
        return extract_insider_content
    return None

def fetch_html_content(url):
    """通用网页内容获取函数"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return None

# ======================
# 各网站专用解析函数
# ======================
def extract_yahoo_content(url):
    """Yahoo Finance正文提取"""
    html = fetch_html_content(url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    # 定位新闻正文所在的容器
    body_div = soup.find('div', class_='body yf-tsvcyu')
    if not body_div:
        return None
    # 提取所有段落文本
    paragraphs = body_div.find_all('p')
    content = '\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
    return content if content else None

def extract_insider_content(url):
    """Business Insider正文提取"""
    html = fetch_html_content(url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    # 查找包含JSON-LD数据的<script>标签
    script_tag = soup.find('script', type='application/ld+json')
    if not script_tag:
        return None
    try:
        # 解析JSON数据
        json_data = json.loads(script_tag.string)
        # 提取articleBody字段
        article_body = json_data.get('articleBody', '')
        return article_body if article_body else None
    except json.JSONDecodeError:
        print("Failed to parse JSON data.")
        return None

def extract_cnbc_content(url):
    """CNBC正文提取"""
    html = fetch_html_content(url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    # CNBC的正文通常在<article class='ArticleBody-articleBody'>中
    article_body = soup.find('article', class_='ArticleBody-articleBody')
    return article_body.get_text(separator='\n') if article_body else None


