import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

# ======================
# 通用函数
# ======================
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
    # Yahoo的正文通常位于<div class='caas-body'>中
    content_div = soup.find('div', class_='caas-body')
    return content_div.get_text(separator='\n') if content_div else None

def extract_insider_content(url):
    """Business Insider正文提取"""
    html = fetch_html_content(url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    # Insider的正文通常在<div class='news-content-post'>中
    content_div = soup.find('div', class_='news-content-post')
    return content_div.get_text(separator='\n') if content_div else None

def extract_cnbc_content(url):
    """CNBC正文提取"""
    html = fetch_html_content(url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    # CNBC的正文通常在<article class='ArticleBody-articleBody'>中
    article_body = soup.find('article', class_='ArticleBody-articleBody')
    return article_body.get_text(separator='\n') if article_body else None


def process_links():
    # 数据加载
    try:
        archive_df = pd.read_pickle("data/news_archive.pkl")
    except FileNotFoundError:
        print("Error: news_archive.pkl file not found")
        return pd.DataFrame()
    # 记录筛选
    now = pd.Timestamp.now(tz='UTC')
    time_threshold = now - pd.Timedelta(hours=4)
    # 创建布尔筛选条件
    time_condition = archive_df['utc_time'] >= time_threshold.tz_convert(None)
    summ_condition = ~archive_df['if_summ']
    valid_time_condition = archive_df['utc_time'].notna()
    to_process = archive_df.loc[
        time_condition & summ_condition & valid_time_condition
    ].copy()
    # 内容提取
    def select_parser(url):
        """根据URL模式选择解析器"""
        if url.startswith("https://finance.yahoo.com/news/"):
            return extract_yahoo_content
        if url.startswith("https://www.cnbc.com/20"):
            return extract_cnbc_content
        if url.startswith("https://www.businessinsider.com/"):
            return extract_insider_content
        return None
    # 遍历处理
    contents = []
    for link in to_process['link']:
        parser = select_parser(link)
        if not parser:
            contents.append(None)
            continue
        contents.append(parser(link))
    to_process['content'] = contents
    # ======================
    # 数据保存
    # ======================
    # 只保留成功提取的记录
    result_df = to_process[to_process['content'].notna()].copy()
    # 将结果合并回原始数据（标记已处理）
    if not result_df.empty:
        archive_df.loc[result_df.index, 'if_summ'] = True
        archive_df.to_pickle("data/news_archive.pkl")
    return result_df[['link', 'content']]

# 使用示例
# processed_df = process_links()


# ======================
# 主处理流程
# ======================
def process_links(df):
    # 创建解析器映射字典
    PARSER_MAP = {
        'yahoo': extract_yahoo_content,
        'insider': extract_insider_content,
        'cnbc': extract_cnbc_content
    }
    # 筛选需要处理的记录
    now = datetime.now()
    time_threshold = now - timedelta(hours=24)
    mask = (pd.to_datetime(df['published']) >= time_threshold) & (~df['if_summ'])
    to_process = df.loc[mask].copy()
    # 初始化结果列
    to_process['content'] = None
    # 遍历处理每个链接
    for idx, row in to_process.iterrows():
        source_key = row['source'].lower()
        if 'yahoo' in source_key:
            parser = PARSER_MAP['yahoo']
        elif 'insider' in source_key:
            parser = PARSER_MAP['insider']
        elif 'cnbc' in source_key:
            parser = PARSER_MAP['cnbc']
        else:
            continue
        content = parser(row['link'])
        to_process.at[idx, 'content'] = content
    return to_process[['link', 'content']]

# 使用示例
# processed_df = process_links(archive_df)


