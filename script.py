import re
import requests
from bs4 import BeautifulSoup
import pandas as pd


# ======================
# 增强型内容提取流程
# ======================
=======
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ======================
# 摘要生成函数
# ======================
def generate_summary(content: str) -> Optional[str]:
    """
    调用DeepSeek API生成摘要
    返回格式："要点1\n要点2\n要点3"
    """
    if not content or len(content) < 100:  # 过滤无效内容
        print("内容过短，跳过摘要生成")
        return None
    payload = {
        "text": content,
        "parameters": {
            "length": "brief",  # 可选项：brief/standard/detailed
            "style": "bullet_points",  # 可选项：paragraph/bullet_points
            "language": "english"  # 根据内容语言调整
        }
    }
    try:
        response = requests.post(
            API_ENDPOINT,
            json=payload,
            headers=HEADERS,
            timeout=15
        )
        response.raise_for_status()
        # 解析响应
        summary = "\n".join(response.json()["points"])  # 假设响应格式为 {"points": [...]}
        return summary.strip()
    except requests.exceptions.RequestException as e:
        print(f"API请求失败: {str(e)}")
        return None
    except KeyError:
        print("响应格式解析失败")
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


