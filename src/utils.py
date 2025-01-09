import os
from datetime import datetime
from typing import Dict, Any

def update_readme(analysis_results: Dict[str, Any]) -> None:
    """更新 README.md 文件"""
    template = f"""# Solana Token Tracker

最后更新时间: {analysis_results['timestamp']}

## 交易统计

- 总交易量: {analysis_results['volume_stats']['total_volume']:.4f} SOL
- 平均交易量: {analysis_results['volume_stats']['avg_volume']:.4f} SOL
- 最大交易量: {analysis_results['volume_stats']['max_volume']:.4f} SOL
- 最小交易量: {analysis_results['volume_stats']['min_volume']:.4f} SOL

## 交易详情

- 总交易数: {analysis_results['transaction_stats']['total_transactions']:,}
- 成功交易数: {analysis_results['transaction_stats']['successful_transactions']:,}
- 平均交易费用: {analysis_results['transaction_stats']['avg_fee']:.6f} SOL
- 交易成功率: {analysis_results['transaction_stats']['success_rate']:.2f}%

## 图表

交易量和交易数量分布图可在 `{analysis_results['chart_path']}` 查看。

> 数据来源: [Solscan](https://solscan.io)
"""

    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(template)