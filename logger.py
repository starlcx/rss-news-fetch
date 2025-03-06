# 新增 logger.py 作为日志模块
import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """配置并返回模块专属日志记录器"""
    logger = logging.getLogger(name or __name__)
    #logger.setLevel(logging.DEBUG)  # 设置最低捕获级别
    logger.setLevel(logging.INFO)  # 设置最低捕获级别
    # 避免重复添加handler
    if logger.handlers:
        return logger
    # 统一日志格式
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)-7s] [%(filename)-15s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # 控制台输出（INFO级别）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    # 文件输出（DEBUG级别）
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_dir / "finance_news.log")
    #file_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

