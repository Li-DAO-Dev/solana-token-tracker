import logging
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR, TOKEN_MINT, RPC_URL
from src.fetcher import TokenDataFetcher
from src.processor import EnhancedAnalyzer
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # 创建数据获取器
        # 创建实例
        #fetcher = TokenDataFetcher(RPC_URL)

        # 获取代币持有者
        logger.info("开始获取数据...")
        #top_holders = fetcher.get_top_holders(TOKEN_MINT)
        #print(top_holders)

        # 保存交易记录
        #fetcher.save_transactions_to_json(top_holders, RAW_DATA_DIR)
        logger.info("处理数据...")
        # 创建数据处理器
        processor = EnhancedAnalyzer()
        processor.process_data(RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR)
        
    except KeyboardInterrupt:
        logger.info("程序被用户中断")

if __name__ == "__main__":
    main()
