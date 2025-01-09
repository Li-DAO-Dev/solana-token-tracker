import os
import sys
import logging
from datetime import datetime
from src.fetcher import SplTokenDataFetcher
from src.analyzer import TokenAnalyzer
from src.utils import update_readme

# 配置日志
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"token_tracker_{datetime.now().strftime('%Y%m%d')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def create_data_directories():
    """创建必要的数据目录"""
    directories = [
        "data",
        "data/raw",
        "data/processed",
        "data/reports"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logging.info(f"确保目录存在: {directory}")

def main():
    try:
        # 设置日志
        setup_logging()
        logging.info("开始运行 Solana Token Tracker")
        
        # 创建必要的目录
        create_data_directories()
        
        # 配置参数
        TOKEN_ADDRESS = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC token address
        DATA_DIR = "data"
        
        # 初始化数据获取器
        logging.info(f"初始化数据获取器，Token地址: {TOKEN_ADDRESS}")
        fetcher = SplTokenDataFetcher(TOKEN_ADDRESS, DATA_DIR)
        
        # 获取交易数据
        logging.info("开始获取交易数据...")
        transactions_df = fetcher.fetch_daily_transactions()
        
        if transactions_df.empty:
            logging.warning("未获取到交易数据")
            return
            
        logging.info(f"成功获取 {len(transactions_df)} 条交易记录")
        
        # 分析数据
        logging.info("开始分析数据...")
        analyzer = TokenAnalyzer(transactions_df)
        analysis_results = analyzer.generate_report()
        
        # 更新 README
        logging.info("更新 README.md...")
        update_readme(analysis_results)
        
        logging.info("数据处理完成")
        
        # 返回分析结果，可用于进一步处理或测试
        return analysis_results
        
    except Exception as e:
        logging.error(f"处理过程中发生错误: {str(e)}", exc_info=True)
        raise e

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("程序执行失败", exc_info=True)
        sys.exit(1)