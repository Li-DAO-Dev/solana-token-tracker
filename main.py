import os
from datetime import datetime
from src.fetcher import SplTokenDataFetcher
from src.analyzer import TokenAnalyzer
from github import Github
import base64
from dotenv import load_dotenv

def update_readme(stats: dict, repo_name: str):
    """更新 GitHub README"""
    load_dotenv()
    g = Github(os.getenv('GITHUB_TOKEN'))
    repo = g.get_repo(repo_name)
    
    # 生成 README 内容
    readme_content = f"""# Solana Token Tracker

自动跟踪 Solana 代币交易数据分析报告

## 最新统计数据
*更新时间: {stats['last_update']}*

### 24小时数据
- 交易数量: {stats['last_24h_transactions']}
- 交易总量: {stats['last_24h_volume']:.2f}
- 活跃地址数: {stats['last_24h_unique_addresses']}

### 总体数据
- 总交易数: {stats['total_transactions']}
- 总交易量: {stats['total_volume']:.2f}
- 总地址数: {stats['unique_addresses']}
- 交易成功率: {stats['success_rate']:.2f}%

## 图表分析
![活动分析](data/reports/activity_charts.png)

## 关于
- 数据每24小时更新一次
- 使用 GitHub Actions 自动化部署
- 数据源: Solana RPC API
"""
    
    try:
        # 获取现有的 README
        contents = repo.get_contents("README.md")
        repo.update_file(
            contents.path,
            f"Update README with latest stats {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            readme_content,
            contents.sha
        )
    except:
        # 如果 README 不存在，创建新的
        repo.create_file(
            "README.md",
            f"Initial README commit {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            readme_content
        )

def main():
    # 初始化获取器和分析器
    token_address = "HhUVkZ1qz8vfMqZDemLyxBFxrHFKVSYAk7a6227Lpump"
    fetcher = SplTokenDataFetcher(token_address)
    analyzer = TokenAnalyzer()
    
    try:
        # 获取新数据
        fetcher.fetch_daily_transactions()
        
        # 合并所有数据
        fetcher.merge_all_data()
        
        # 生成报告
        stats = analyzer.generate_report()
        
        if stats:
            # 更新 GitHub README
            update_readme(stats, "你的GitHub用户名/仓库名")
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    main()