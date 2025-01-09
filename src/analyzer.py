import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, Any
import os

class TokenAnalyzer:
    def __init__(self, transactions_df: pd.DataFrame):
        self.df = transactions_df
        self.prepare_data()
        
    def prepare_data(self):
        """预处理数据"""
        if not self.df.empty:
            self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
            self.df['hour'] = self.df['timestamp'].dt.hour
            self.df['date'] = self.df['timestamp'].dt.date
            # 将 lamport 转换为 SOL (1 SOL = 1e9 lamport)
            self.df['sol_amount'] = self.df['lamport'] / 1e9
            
    def analyze_volume(self) -> Dict[str, Any]:
        """分析交易量"""
        if self.df.empty:
            return {
                "total_volume": 0,
                "avg_volume": 0,
                "max_volume": 0,
                "min_volume": 0
            }
            
        volume_stats = {
            "total_volume": self.df['sol_amount'].sum(),
            "avg_volume": self.df['sol_amount'].mean(),
            "max_volume": self.df['sol_amount'].max(),
            "min_volume": self.df['sol_amount'].min()
        }
        return volume_stats
        
    def analyze_transactions(self) -> Dict[str, Any]:
        """分析交易数据"""
        if self.df.empty:
            return {
                "total_transactions": 0,
                "successful_transactions": 0,
                "avg_fee": 0,
                "success_rate": 0
            }
            
        successful_txs = self.df['success'].sum()
        total_txs = len(self.df)
        
        tx_stats = {
            "total_transactions": total_txs,
            "successful_transactions": successful_txs,
            "avg_fee": self.df['fee'].mean() / 1e9,  # 转换为 SOL
            "success_rate": (successful_txs / total_txs * 100) if total_txs > 0 else 0
        }
        return tx_stats
        
    def generate_hourly_chart(self) -> str:
        """生成小时交易量图表"""
        if self.df.empty:
            return ""
            
        # 确保reports目录存在
        os.makedirs("data/reports", exist_ok=True)
            
        hourly_volume = self.df.groupby('hour').agg({
            'sol_amount': 'sum',
            'signature': 'count'
        }).reset_index()
        
        # 创建双轴图表
        fig = go.Figure()
        
        # 添加交易量柱状图
        fig.add_trace(go.Bar(
            x=hourly_volume['hour'],
            y=hourly_volume['sol_amount'],
            name='交易量 (SOL)',
            yaxis='y'
        ))
        
        # 添加交易数量折线图
        fig.add_trace(go.Scatter(
            x=hourly_volume['hour'],
            y=hourly_volume['signature'],
            name='交易数量',
            yaxis='y2'
        ))
        
        # 更新布局
        fig.update_layout(
            title='每小时交易量和交易数量分布',
            xaxis_title='小时',
            yaxis_title='交易量 (SOL)',
            yaxis2=dict(
                title='交易数量',
                overlaying='y',
                side='right'
            ),
            barmode='group'
        )
        
        chart_path = "data/reports/hourly_volume.html"
        fig.write_html(chart_path)
        return chart_path
        
    def generate_report(self) -> Dict[str, Any]:
        """生成完整分析报告"""
        volume_stats = self.analyze_volume()
        tx_stats = self.analyze_transactions()
        chart_path = self.generate_hourly_chart()
        
        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "volume_stats": volume_stats,
            "transaction_stats": tx_stats,
            "chart_path": chart_path
        }
        
        return report