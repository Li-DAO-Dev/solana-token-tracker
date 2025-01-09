import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time
import json
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from base58 import b58encode
import base64

class SplTokenDataFetcher:
  def __init__(self, token_address: str, data_dir: str):
      self.token_address = token_address
      self.data_dir = data_dir
      self.raw_data_dir = os.path.join(data_dir, 'raw')
      self.processed_data_dir = os.path.join(data_dir, 'processed')
      self.metadata_file = os.path.join(data_dir, 'metadata.json')
      
      # Solana RPC 节点配置
      self.rpc_endpoints = [
          "https://api.mainnet-beta.solana.com",
          "https://solana-api.projectserum.com",
          "https://rpc.ankr.com/solana"
      ]
      self.current_endpoint_index = 0
      self.max_retries = 3
      self.retry_delay = 2
      self.batch_size = 100
      
      # 创建必要的目录
      os.makedirs(self.raw_data_dir, exist_ok=True)
      os.makedirs(self.processed_data_dir, exist_ok=True)
      
  def _make_rpc_request(self, method: str, params: List[Any], retry_count: int = 0) -> Optional[Dict]:
      """发送 RPC 请求并处理重试逻辑"""
      try:
          endpoint = self.rpc_endpoints[self.current_endpoint_index]
          headers = {"Content-Type": "application/json"}
          payload = {
              "jsonrpc": "2.0",
              "id": 1,
              "method": method,
              "params": params
          }
          
          response = requests.post(
              endpoint,
              headers=headers,
              json=payload,
              timeout=30
          )
          response.raise_for_status()
          result = response.json()
          
          if "error" in result:
              raise Exception(f"RPC错误: {result['error']}")
              
          return result.get("result")
          
      except Exception as e:
          # 如果当前节点失败，尝试切换到下一个节点
          if self.current_endpoint_index + 1 < len(self.rpc_endpoints):
              self.current_endpoint_index += 1
              print(f"切换到下一个RPC节点: {self.rpc_endpoints[self.current_endpoint_index]}")
              return self._make_rpc_request(method, params, retry_count)
          
          # 如果所有节点都尝试过，进行重试
          if retry_count < self.max_retries:
              sleep_time = self.retry_delay * (2 ** retry_count)
              print(f"请求失败，{sleep_time}秒后重试: {str(e)}")
              time.sleep(sleep_time)
              # 重置节点索引
              self.current_endpoint_index = 0
              return self._make_rpc_request(method, params, retry_count + 1)
          
          print(f"达到最大重试次数，请求失败: {str(e)}")
          return None

  def _load_metadata(self) -> Dict:
      """加载元数据"""
      try:
          if os.path.exists(self.metadata_file):
              with open(self.metadata_file, 'r') as f:
                  return json.load(f)
      except Exception as e:
          print(f"读取元数据文件失败: {str(e)}")
      return {"last_update": None, "last_signature": None}

  def _save_metadata(self, metadata: Dict) -> None:
      """保存元数据"""
      try:
          with open(self.metadata_file, 'w') as f:
              json.dump(metadata, f, indent=2)
      except Exception as e:
          print(f"保存元数据文件失败: {str(e)}")

  def _get_signatures(self, before: str = "", limit: int = 1000) -> List[str]:
      """获取交易签名列表"""
      params = [
          self.token_address,
          {
              "limit": limit,
              "commitment": "confirmed"
          }
      ]
      
      if before:
          params[1]["before"] = before
          
      result = self._make_rpc_request("getSignaturesForAddress", params)
      if not result:
          return []
          
      return [tx["signature"] for tx in result]

  def _get_transaction_details(self, signature: str) -> Optional[Dict]:
      """获取单个交易详情"""
      params = [
          signature,
          {"encoding": "json", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}
      ]
      
      result = self._make_rpc_request("getTransaction", params)
      if not result:
          return None
          
      try:
          return {
              'timestamp': datetime.fromtimestamp(result.get('blockTime', 0)),
              'signature': signature,
              'slot': result.get('slot', 0),
              'success': result.get('meta', {}).get('err') is None,
              'fee': result.get('meta', {}).get('fee', 0),
              'lamport': sum(
                  pre['lamports'] - post['lamports']
                  for pre, post in zip(
                      result.get('meta', {}).get('preBalances', []),
                      result.get('meta', {}).get('postBalances', [])
                  )
              )
          }
      except Exception as e:
          print(f"处理交易 {signature} 详情失败: {str(e)}")
          return None

  def _load_existing_data(self) -> pd.DataFrame:
      """加载现有数据"""
      try:
          csv_files = sorted([f for f in os.listdir(self.raw_data_dir) if f.endswith('.csv')])
          
          if not csv_files:
              return pd.DataFrame()
              
          dfs = []
          with ThreadPoolExecutor(max_workers=4) as executor:
              futures = []
              for file in csv_files:
                  file_path = os.path.join(self.raw_data_dir, file)
                  futures.append(executor.submit(pd.read_csv, file_path))
                  
              for future in as_completed(futures):
                  try:
                      df = future.result()
                      df['timestamp'] = pd.to_datetime(df['timestamp'])
                      dfs.append(df)
                  except Exception as e:
                      print(f"读取CSV文件失败: {str(e)}")
                      
          return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
          
      except Exception as e:
          print(f"加载现有数据失败: {str(e)}")
          return pd.DataFrame()

  def _save_transactions_batch(self, transactions: List[Dict], date: datetime.date) -> None:
      """批量保存交易数据"""
      try:
          file_path = os.path.join(self.raw_data_dir, f'transactions_{date}.csv')
          df = pd.DataFrame(transactions)
          df.to_csv(file_path, index=False)
          print(f"成功保存 {len(transactions)} 条交易到: {file_path}")
      except Exception as e:
          print(f"保存交易数据失败: {str(e)}")

  def initialize_historical_data(self, days: int = 30) -> pd.DataFrame:
      """初始化历史数据"""
      print(f"开始获取最近 {days} 天的历史数据...")
      
      all_transactions = []
      last_signature = ""
      total_fetched = 0
      cutoff_time = datetime.now() - timedelta(days=days)
      
      try:
          while True:
              signatures = self._get_signatures(before=last_signature, limit=50)
              if not signatures:
                  break
                  
              # 使用线程池并行获取交易详情
              batch_data = []
              with ThreadPoolExecutor(max_workers=5) as executor:
                  futures = {
                      executor.submit(self._get_transaction_details, sig): sig 
                      for sig in signatures
                  }
                  
                  for future in as_completed(futures):
                      try:
                          tx_data = future.result()
                          if tx_data and tx_data['timestamp'] >= cutoff_time:
                              batch_data.append(tx_data)
                          elif tx_data and tx_data['timestamp'] < cutoff_time:
                              break
                      except Exception as e:
                          print(f"处理交易详情失败: {str(e)}")
              
              if not batch_data:
                  break
                  
              all_transactions.extend(batch_data)
              total_fetched += len(batch_data)
              
              print(f"已获取 {total_fetched} 条交易记录... ({batch_data[-1]['timestamp'].strftime('%Y-%m-%d')})")
              
              last_signature = signatures[-1]
              time.sleep(0.1)  # 轻微限速
              
          df = pd.DataFrame(all_transactions)
          
          if not df.empty:
              for date, group in df.groupby(df['timestamp'].dt.date):
                  self._save_transactions_batch(group.to_dict('records'), date)
                  
          self._save_metadata({
              "last_update": datetime.now().strftime("%Y-%m-%d"),
              "last_signature": last_signature if signatures else None
          })
          
          return df
          
      except Exception as e:
          print(f"初始化历史数据时发生错误: {str(e)}")
          return pd.DataFrame()

  def fetch_daily_transactions(self) -> pd.DataFrame:
      """获取每日更新的交易数据"""
      try:
          metadata = self._load_metadata()
          last_update = metadata.get("last_update")
          
          if last_update is None:
              return self.initialize_historical_data()
              
          last_update_date = datetime.strptime(last_update, "%Y-%m-%d").date()
          today = datetime.now().date()
          
          if last_update_date >= today:
              print("数据已是最新，无需更新")
              return self._load_existing_data()
              
          print(f"开始获取 {last_update_date} 之后的新增交易数据...")
          
          all_transactions = []
          last_signature = ""
          total_fetched = 0
          
          while True:
              signatures = self._get_signatures(before=last_signature, limit=50)
              if not signatures:
                  break
                  
              batch_data = []
              with ThreadPoolExecutor(max_workers=5) as executor:
                  futures = {
                      executor.submit(self._get_transaction_details, sig): sig 
                      for sig in signatures
                  }
                  
                  for future in as_completed(futures):
                      try:
                          tx_data = future.result()
                          if tx_data and tx_data['timestamp'].date() > last_update_date:
                              batch_data.append(tx_data)
                          elif tx_data and tx_data['timestamp'].date() <= last_update_date:
                              break
                      except Exception as e:
                          print(f"处理交易详情失败: {str(e)}")
              
              if not batch_data:
                  break
                  
              all_transactions.extend(batch_data)
              total_fetched += len(batch_data)
              
              print(f"已获取 {total_fetched} 条新交易记录...")
              
              last_signature = signatures[-1]
              time.sleep(0.1)
              
          new_df = pd.DataFrame(all_transactions)
          
          if not new_df.empty:
              for date, group in new_df.groupby(new_df['timestamp'].dt.date):
                  self._save_transactions_batch(group.to_dict('records'), date)
                  
          self._save_metadata({
              "last_update": today.strftime("%Y-%m-%d"),
              "last_signature": last_signature if signatures else metadata["last_signature"]
          })
          
          return self._load_existing_data()
          
      except Exception as e:
          print(f"获取每日交易数据时发生错误: {str(e)}")
          return self._load_existing_data()