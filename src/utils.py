import os
from datetime import datetime
from typing import Dict, Any

__all__ = ['update_readme']  # 明确声明要导出的函数

def update_readme(analysis_results: Dict[str, Any]) -> None:
    """更新 README.md 文件"""
    template = f"""# Solana Token Tracker

最后更新时间: {analysis_results['timestamp']}

## 交易统计

- 总交易量: {analysis_results['volume_stats']['total_volume']:,.2f}
- 平均交易量: {analysis_results['volume_stats']['avg_volume']:,.2f}
- 最大交易量: {analysis_results['volume_stats']['max_volume']:,.2f}
- 最小交易量: {analysis_results['volume_stats']['min_volume']:,.2f}

## 交易详情

- 总交易数: {analysis_results['transaction_stats']['total_transactions']:,}
- 独立地址数: {analysis_results['transaction_stats']['unique_addresses']:,}
- 平均交易大小: {analysis_results['transaction_stats']['avg_transaction_size']:,.2f}

## 图表

交易量分布图可在 `{analysis_results['chart_path']}` 查看。
"""

    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(template)