import os
import zipfile
import feedparser
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
from logger import setup_logger

# 初始化日志记录器
logger = setup_logger(__name__)

# 配置常量
RSS_SOURCES = {
    "Yahoo Finance": "http://finance.yahoo.com/rss/topstories",
    "Business Insider": "https://www.businessinsider.com/rss",
    "CNBC": "https://www.cnbc.com/id/100003114/device/rss/rss.html"
}
DATA_DIR = "data"
ARCHIVE_FILE = os.path.join(DATA_DIR, "news_archive.pkl")

def setup_data_directory() -> None:
    """初始化数据存储目录"""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        logger.debug(f"Data directory setup: {DATA_DIR}")
    except Exception as e:
        logger.error(f"Failed to create data directory: {str(e)}", exc_info=True)
        raise

def fetch_single_feed(source: str, url: str) -> List[Dict]:
    """获取单个RSS源的数据"""
    try:
        logger.info(f"Fetching feed from {source}")
        feed = feedparser.parse(url)
        if not feed.entries:
            logger.warning(f"No entries found in {source}")
            return []
        return feed.entries
    except Exception as e:
        logger.error(f"Failed to fetch {source}", exc_info=True)
        return []

def process_entry(source: str, entry: Dict) -> Dict:
    """处理单个RSS条目为结构化数据"""
    try:
        processed = {
            'source': source,
            'title': entry.get('title', ''),
            'link': entry.get('link', ''),
            'published': entry.get('published', ''),
            'description': entry.get('description', ''),
            'content': entry.get('content', [{}])[0].get('value', '') if entry.get('content') else '',
            'guid': entry.get('guid', ''),
            'if_summ': False,
            'summary': ''
        }
        logger.debug(f"Processed entry: {processed['title'][:50]}...")
        return processed
    except Exception as e:
        logger.error(f"Failed to process entry: {str(e)}", exc_info=True)
        return {}

def fetch_all_feeds() -> pd.DataFrame:
    """获取并处理所有RSS源数据"""
    all_entries = []
    for source, url in RSS_SOURCES.items():
        entries = fetch_single_feed(source, url)
        all_entries.extend([process_entry(source, entry) for entry in entries])
    if not all_entries:
        logger.warning("No entries found in any feed")
    return pd.DataFrame(all_entries)


def merge_with_archive(new_df: pd.DataFrame) -> pd.DataFrame:
    """合并新旧数据并去重"""
    try:
        if os.path.exists(ARCHIVE_FILE):
            logger.info(f"Merging with existing archive: {ARCHIVE_FILE}")
            archive_df = pd.read_pickle(ARCHIVE_FILE)  # 修改读取方式
            combined_df = pd.concat([archive_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['guid'], keep='last')
            return combined_df
        else:
            logger.info("No existing archive found, creating new one")
            return new_df
    except Exception as e:
        logger.error(f"Failed to merge with archive: {str(e)}", exc_info=True)
        raise

def save_data(new_df: pd.DataFrame) -> None:
    """保存数据到文件系统"""
    try:
        # 保存本次获取数据
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        new_file = os.path.join(DATA_DIR, f"news_{timestamp}.pkl")
        new_df.to_pickle(new_file)  # 保存为pkl
        logger.info(f"Saved new data to: {new_file}")
        # 保存更新后的存档
        archive_df = merge_with_archive(new_df)
        archive_df.to_pickle(ARCHIVE_FILE)
        archive_df.to_csv(ARCHIVE_FILE.replace('.pkl', '.tsv'), sep='\t', index=False)
        logger.info(f"Archive updated: {len(archive_df)} total records")
    except Exception as e:
        logger.error(f"Failed to save data: {str(e)}", exc_info=True)
        raise

def main() -> None:
    """主执行流程"""
    try:
        logger.info("Starting RSS feed aggregation")
        setup_data_directory()
        new_data = fetch_all_feeds()
        if not new_data.empty:
            save_data(new_data)
            logger.info(f"Processing complete: {len(new_data)} new entries")
        else:
            logger.warning("No new entries to process")
    except Exception as e:
        logger.critical("Fatal error in main execution", exc_info=True)
        raise

if __name__ == "__main__":
    main()


