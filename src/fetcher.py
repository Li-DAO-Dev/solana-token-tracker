import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time
import json
from typing import List, Dict, Any, Optional

class SplTokenDataFetcher:
    def __init__(self, token_address: str, data_dir: str):
        self.token_address = token_address
        self.data_dir = data_dir
        self.raw_data_dir = os.path.join(data_dir, 'raw')
        self.processed_data_dir = os.path.join(data_dir, 'processed')
        self.base_url = "https://public-api.solscan.io"
        self.metadata_file = os.path.join(data_dir, 'metadata.json')
        
        # 创建必要的目录
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)
        
    def _load_metadata(self) -> Dict:
        """加载元数据"""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return {
            "last_update": None,
            "last_signature": None
        }
        
    def _save_metadata(self, metadata: Dict) -> None:
        """保存元数据"""
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f)
            
    def _fetch_transactions_page(self, limit: int = 50, before: str = "") -> List[Dict[str, Any]]:
        """获取单页交易数据"""
        endpoint = f"{self.base_url}/account/transactions"
        params = {
            "account": self.token_address,
            "limit": limit
        }
        if before:
            params["before"] = before
            
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        try:
            response = requests.get(endpoint, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API请求失败: {str(e)}")
            return []
            
    def _process_transaction(self, tx: Dict) -> Optional[Dict]:
        """处理单个交易数据"""
        try:
            timestamp = datetime.fromtimestamp(tx.get('blockTime', 0))
            return {
                'timestamp': timestamp,
                'signature': tx.get('signature', ''),
                'slot': tx.get('slot', 0),
                'success': tx.get('status', 'Success') == 'Success',
                'fee': tx.get('fee', 0),
                'lamport': tx.get('lamport', 0)
            }
        except Exception as e:
            print(f"处理交易数据时出错: {str(e)}")
            return None
            
    def _load_existing_data(self) -> pd.DataFrame:
        """加载现有数据"""
        # 获取所有CSV文件并按日期排序
        csv_files = [f for f in os.listdir(self.raw_data_dir) if f.endswith('.csv')]
        csv_files.sort()
        
        if not csv_files:
            return pd.DataFrame()
            
        # 合并所有CSV文件
        dfs = []
        for file in csv_files:
            df = pd.read_csv(os.path.join(self.raw_data_dir, file))
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            dfs.append(df)
            
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        
    def initialize_historical_data(self, days: int = 30) -> pd.DataFrame:
        """初始化历史数据"""
        print(f"开始获取最近 {days} 天的历史数据...")
        
        all_transactions = []
        last_signature = ""
        total_fetched = 0
        cutoff_time = datetime.now() - timedelta(days=days)
        
        while True:
            transactions = self._fetch_transactions_page(limit=50, before=last_signature)
            
            if not transactions:
                break
                
            for tx in transactions:
                tx_data = self._process_transaction(tx)
                if tx_data is None:
                    continue
                    
                # 检查是否超出时间范围
                if tx_data['timestamp'] < cutoff_time:
                    break
                    
                all_transactions.append(tx_data)
                
            total_fetched += len(transactions)
            print(f"已获取 {total_fetched} 条交易记录...")
            
            # 获取最后一笔交易的签名
            if transactions:
                last_signature = transactions[-1].get('signature', '')
                
            # API 限速 (每秒2次请求)
            time.sleep(0.5)
            
            # 检查最后一笔交易是否超出时间范围
            if tx_data['timestamp'] < cutoff_time:
                break
                
        df = pd.DataFrame(all_transactions)
        
        if not df.empty:
            # 按日期分组保存数据
            for date, group in df.groupby(df['timestamp'].dt.date):
                file_path = os.path.join(self.raw_data_dir, f'transactions_{date}.csv')
                group.to_csv(file_path, index=False)
                print(f"保存数据到: {file_path}")
                
        # 更新元数据
        metadata = {
            "last_update": datetime.now().strftime("%Y-%m-%d"),
            "last_signature": last_signature if transactions else None
        }
        self._save_metadata(metadata)
        
        return df
        
    def fetch_daily_transactions(self) -> pd.DataFrame:
        """获取每日更新的交易数据"""
        metadata = self._load_metadata()
        last_update = metadata.get("last_update")
        
        # 检查是否需要初始化数据
        if last_update is None:
            print("未找到历史数据，开始初始化...")
            return self.initialize_historical_data()
            
        # 检查是否需要更新
        last_update_date = datetime.strptime(last_update, "%Y-%m-%d").date()
        today = datetime.now().date()
        
        if last_update_date >= today:
            print("数据已是最新，无需更新")
            return self._load_existing_data()
            
        print("开始获取新增交易数据...")
        all_transactions = []
        last_signature = ""
        total_fetched = 0
        
        while True:
            transactions = self._fetch_transactions_page(limit=50, before=last_signature)
            
            if not transactions:
                break
                
            for tx in transactions:
                tx_data = self._process_transaction(tx)
                if tx_data is None:
                    continue
                    
                # 只获取上次更新之后的数据
                if tx_data['timestamp'].date() <= last_update_date:
                    break
                    
                all_transactions.append(tx_data)
                
            total_fetched += len(transactions)
            print(f"已获取 {total_fetched} 条新交易记录...")
            
            if transactions:
                last_signature = transactions[-1].get('signature', '')
            
            # API 限速
            time.sleep(0.5)
            
            # 检查是否已获取到上次更新的数据
            if tx_data['timestamp'].date() <= last_update_date:
                break
                
        new_df = pd.DataFrame(all_transactions)
        
        if not new_df.empty:
            # 按日期分组保存新数据
            for date, group in new_df.groupby(new_df['timestamp'].dt.date):
                file_path = os.path.join(self.raw_data_dir, f'transactions_{date}.csv')
                group.to_csv(file_path, index=False)
                print(f"保存新数据到: {file_path}")
                
        # 更新元数据
        metadata["last_update"] = today.strftime("%Y-%m-%d")
        metadata["last_signature"] = last_signature if transactions else metadata["last_signature"]
        self._save_metadata(metadata)
        
        # 返回所有数据
        return self._load_existing_data()