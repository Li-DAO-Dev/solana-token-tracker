import os
from datetime import datetime
from src.fetcher import SplTokenDataFetcher
from src.analyzer import TokenAnalyzer
from src.utils import update_readme

def main():
    # 配置参数
    token_address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
    data_dir = "data"
    
    try:
        # 初始化数据获取器
        fetcher = SplTokenDataFetcher(token_address, data_dir)
        
        # 获取最新数据
        transactions_df = fetcher.fetch_daily_transactions()
        
        # 初始化分析器
        analyzer = TokenAnalyzer(transactions_df)
        
        # 生成分析报告
        analysis_results = analyzer.generate_report()
        
        # 更新 README
        update_readme(analysis_results)
        
        print("数据更新和分析完成！")
        
    except Exception as e:
        print(f"发生错误: {str(e)}")
        raise e

if __name__ == "__main__":
    main()