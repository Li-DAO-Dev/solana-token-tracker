import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, Any

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
            "total_volume": self.df['amount'].sum(),
            "avg_volume": self.df['amount'].mean(),
            "max_volume": self.df['amount'].max(),
            "min_volume": self.df['amount'].min()
        }
        return volume_stats
        
    def analyze_transactions(self) -> Dict[str, Any]:
        """分析交易数据"""
        if self.df.empty:
            return {
                "total_transactions": 0,
                "unique_addresses": 0,
                "avg_transaction_size": 0
            }
            
        tx_stats = {
            "total_transactions": len(self.df),
            "unique_addresses": len(set(self.df['from_address'].unique()) | 
                                  set(self.df['to_address'].unique())),
            "avg_transaction_size": self.df['amount'].mean()
        }
        return tx_stats
        
    def generate_hourly_chart(self) -> str:
        """生成小时交易量图表"""
        if self.df.empty:
            return ""
            
        hourly_volume = self.df.groupby('hour')['amount'].sum().reset_index()
        
        fig = px.bar(hourly_volume, 
                    x='hour', 
                    y='amount',
                    title='Hourly Transaction Volume')
        
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
