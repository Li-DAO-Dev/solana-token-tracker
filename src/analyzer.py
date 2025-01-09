import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta
import json

class TokenAnalyzer:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.processed_dir = os.path.join(data_dir, "processed")
        self.reports_dir = os.path.join(data_dir, "reports")
        self.ensure_dirs()
        
    def ensure_dirs(self):
        if not os.path.exists(self.reports_dir):
            os.makedirs(self.reports_dir)
            
    def load_data(self) -> pd.DataFrame:
        """加载处理后的数据"""
        data_file = os.path.join(self.processed_dir, "all_transactions.csv")
        if not os.path.exists(data_file):
            return None
        df = pd.read_csv(data_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
        
    def generate_daily_stats(self, df: pd.DataFrame) -> dict:
        """生成每日统计数据"""
        if df is None or df.empty:
            return {}
            
        # 最近24小时的数据
        last_24h = df[df['timestamp'] >= datetime.now() - timedelta(days=1)]
        
        # 计算统计数据
        stats = {
            "total_transactions": len(df),
            "last_24h_transactions": len(last_24h),
            "total_volume": abs(df['amount_change']).sum(),
            "last_24h_volume": abs(last_24h['amount_change']).sum(),
            "unique_addresses": df['account'].nunique(),
            "last_24h_unique_addresses": last_24h['account'].nunique(),
            "success_rate": (df['success'].mean() * 100),
            "last_update": datetime.now().isoformat()
        }
        
        return stats
        
    def generate_charts(self, df: pd.DataFrame):
        """生成分析图表"""
        if df is None or df.empty:
            return None
            
        # 按小时统计交易量
        df['hour'] = df['timestamp'].dt.floor('H')
        hourly_volume = df.groupby('hour').agg({
            'amount_change': lambda x: abs(x).sum(),
            'signature': 'count'
        }).reset_index()
        
        # 创建子图
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Hourly Transaction Volume', 'Hourly Transaction Count')
        )
        
        # 添加交易量图表
        fig.add_trace(
            go.Scatter(
                x=hourly_volume['hour'],
                y=hourly_volume['amount_change'],
                mode='lines',
                name='Volume'
            ),
            row=1, col=1
        )
        
        # 添加交易数量图表
        fig.add_trace(
            go.Scatter(
                x=hourly_volume['hour'],
                y=hourly_volume['signature'],
                mode='lines',
                name='Count'
            ),
            row=2, col=1
        )
        
        # 更新布局
        fig.update_layout(
            height=800,
            showlegend=True,
            title_text="Token Activity Analysis"
        )
        
        # 保存图表
        fig.write_html(os.path.join(self.reports_dir, "activity_charts.html"))
        fig.write_image(os.path.join(self.reports_dir, "activity_charts.png"))
        
    def save_stats(self, stats: dict):
        """保存统计数据"""
        with open(os.path.join(self.reports_dir, "stats.json"), 'w') as f:
            json.dump(stats, f, indent=2)
            
    def generate_report(self):
        """生成完整报告"""
        df = self.load_data()
        if df is not None:
            stats = self.generate_daily_stats(df)
            self.generate_charts(df)
            self.save_stats(stats)
            return stats
        return None
16 分钟前
