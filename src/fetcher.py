import pandas as pd
import json
from datetime import datetime
import os
from typing import Dict, List, Any
import time
import requests
from tqdm import tqdm
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class SplTokenDataFetcher:
    def __init__(self, token_address: str, data_dir: str = "data"):
        self.token_address = token_address
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")
        self.rpc_url = "https://api.mainnet-beta.solana.com"
        self.max_retries = 5
        self.session = self._create_session()
        self.ensure_dirs()
        self.checkpoint_file = os.path.join(self.data_dir, "checkpoint.json")
        
    def ensure_dirs(self):
        """确保所需的目录存在"""
        for dir_path in [self.data_dir, self.raw_dir, self.processed_dir]:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

    # ... [之前的其他方法保持不变] ...

    def fetch_daily_transactions(self):
        """获取最近24小时的交易数据"""
        print(f"开始获取 {self.token_address} 的最新交易数据...")
        
        # 获取最新的检查点
        checkpoint = self.load_checkpoint()
        if checkpoint:
            last_known_tx = checkpoint["last_signature"]
        else:
            last_known_tx = None
        
        # 获取新交易
        new_transactions = []
        current_signature = None
        
        while True:
            signatures = self.get_signatures(before=current_signature, limit=50)
            if not signatures:
                break
                
            for sig_info in signatures:
                # 如果遇到已知的交易，说明已经获取到所有新交易
                if sig_info["signature"] == last_known_tx:
                    break
                    
                tx_data = self.get_transaction(sig_info["signature"])
                parsed_tx = self.parse_transaction_data(tx_data)
                if parsed_tx:
                    new_transactions.append(parsed_tx)
                
                time.sleep(1)  # 请求间隔
                
            if not signatures or sig_info["signature"] == last_known_tx:
                break
                
            current_signature = signatures[-1]["signature"]
            
        # 保存新数据
        if new_transactions:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.raw_dir}/transactions_{timestamp}.csv"
            self.save_transactions_to_csv(new_transactions, filename)
            
            # 更新检查点
            self.save_checkpoint(new_transactions[0]["signature"], len(new_transactions))
            
        return new_transactions

    def merge_all_data(self):
        """合并所有原始数据文件"""
        all_files = [f for f in os.listdir(self.raw_dir) if f.endswith('.csv')]
        if not all_files:
            return None
            
        dfs = []
        for filename in all_files:
            df = pd.read_csv(os.path.join(self.raw_dir, filename))
            dfs.append(df)
            
        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            merged_df = merged_df.drop_duplicates(subset=['signature'])
            merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])
            merged_df = merged_df.sort_values('timestamp', ascending=False)
            
            # 保存合并后的数据
            merged_file = os.path.join(self.processed_dir, "all_transactions.csv")
            merged_df.to_csv(merged_file, index=False)
            
            return merged_df
        return None