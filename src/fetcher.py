from typing import Dict, List, Tuple, Optional, Any
import os
import json
from datetime import datetime
import requests
import time

class TokenDataFetcher:
    def __init__(self, rpc_url: str, data_dir: str = "transactions_data"):
        """
        初始化 TokenDataFetcher
        
        Args:
            rpc_url: Solana RPC 节点的 URL
            data_dir: 数据存储目录
        """
        self.rpc_url = rpc_url
        self.data_dir = data_dir
        self.headers = {"Content-Type": "application/json"}
        self.token_program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        
        # 确保数据目录存在
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            
        # 状态文件路径
        self.state_file = os.path.join(data_dir, "fetch_state.json")

    def get_top_holders(self, mint_address: str, limit: int = 100) -> List[Tuple[str, float]]:
        """
        获取代币的前N大持有者
        
        Args:
            mint_address: 代币的 mint 地址
            limit: 返回的持有者数量
            
        Returns:
            List[Tuple[str, float]]: 持有者地址和余额的列表
        """
        try:
            # 构建 RPC 请求
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getProgramAccounts",
                "params": [
                    self.token_program_id,
                    {
                        "encoding": "jsonParsed",
                        "filters": [
                            {
                                "dataSize": 165  # Token account size
                            },
                            {
                                "memcmp": {
                                    "offset": 0,
                                    "bytes": mint_address
                                }
                            }
                        ]
                    }
                ]
            }

            # 发送请求
            response = requests.post(self.rpc_url, json=payload, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                raise Exception(f"RPC错误: {data['error']}")

            # 解析响应
            holders = []
            for account in data.get("result", []):
                parsed_data = account.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
                address = account.get("pubkey")
                balance = float(parsed_data.get("tokenAmount", {}).get("uiAmount", 0))
                
                if balance > 0:
                    holders.append((address, balance))

            # 按余额排序并返回前N个
            holders.sort(key=lambda x: x[1], reverse=True)
            return holders[:limit]

        except Exception as e:
            print(f"获取持有者列表时出错: {e}")
            return []

    def load_fetch_state(self) -> Dict:
        """
        加载上次获取数据的状态
        
        Returns:
            Dict: 包含上次更新时间和地址信息的状态字典
        """
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"加载状态文件时出错: {e}")
        return {
            "last_update": None,
            "addresses": {}
        }

    def save_fetch_state(self, state: Dict) -> None:
        """
        保存数据获取状态
        
        Args:
            state: 状态信息字典
        """
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存状态文件时出错: {e}")

    def get_latest_signature(self, address: str) -> Optional[str]:
        """
        获取地址的最新交易签名
        
        Args:
            address: 要查询的地址
            
        Returns:
            Optional[str]: 最新交易的签名
        """
        transactions = self.get_recent_transactions(address, limit=1)
        if transactions and len(transactions) > 0:
            return transactions[0].get("signature")
        return None

    def get_recent_transactions(self, address: str, limit: int = 100, until: Optional[str] = None) -> List[Dict]:
        """
        获取地址的最近交易
        
        Args:
            address: 要查询的地址
            limit: 返回的交易数量
            until: 截止的交易签名
            
        Returns:
            List[Dict]: 交易列表
        """
        try:
            params = {
                "limit": limit
            }
            if until:
                params["until"] = until

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [address, params]
            }

            response = requests.post(self.rpc_url, json=payload, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                raise Exception(f"RPC错误: {data['error']}")

            return data.get("result", [])

        except Exception as e:
            print(f"获取交易列表时出错: {e}")
            return []

    def get_transaction_details(self, signature: str) -> Dict:
        """
        获取交易详情
        
        Args:
            signature: 交易签名
            
        Returns:
            Dict: 交易详情
        """
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [
                    signature,
                    {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
                ]
            }

            response = requests.post(self.rpc_url, json=payload, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                raise Exception(f"RPC错误: {data['error']}")

            return data.get("result", {})

        except Exception as e:
            print(f"获取交易详情时出错: {e}")
            return {}

    def fetch_new_transactions(self, address: str, last_signature: Optional[str] = None) -> List[Dict]:
        """
        获取地址的新交易记录
        
        Args:
            address: 要查询的地址
            last_signature: 上次获取的最后一个交易签名
            
        Returns:
            List[Dict]: 新的交易记录列表
        """
        all_transactions = []
        current_signature = None
        
        while True:
            # 构建查询参数
            params = {"limit": 100}
            if current_signature:
                params["until"] = current_signature
            
            transactions = self.get_recent_transactions(address, **params)
            
            if not transactions:
                break
                
            for tx in transactions:
                signature = tx.get("signature")
                # 如果遇到上次的最后一个交易，停止获取
                if signature == last_signature:
                    return all_transactions
                    
                details = self.get_transaction_details(signature)
                all_transactions.append({
                    "交易签名": signature,
                    "交易详情": details
                })
                
            current_signature = transactions[-1].get("signature")
            
            # 如果返回的交易数量少于请求的数量，说明已经获取完所有交易
            if len(transactions) < 100:
                break
                
        return all_transactions

    def update_holder_data(self, mint_address: str, force_full_update: bool = False) -> None:
        """
        更新代币持有者数据，支持增量更新
        
        Args:
            mint_address: 代币的 mint 地址
            force_full_update: 是否强制全量更新
        """
        print(f"开始{'全量' if force_full_update else '增量'}更新数据...")
        
        # 获取当前状态
        state = self.load_fetch_state()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 获取最新的持有者列表
        top_holders = self.get_top_holders(mint_address)
        
        # 更新索引文件数据
        index_data = {
            "更新时间": current_time,
            "地址列表": []
        }
        
        for address, balance in top_holders:
            print(f"处理地址 {address}...")
            
            # 获取该地址的历史状态
            address_state = state["addresses"].get(address, {})
            last_signature = None if force_full_update else address_state.get("last_signature")
            
            # 获取新交易
            new_transactions = self.fetch_new_transactions(address, last_signature)
            
            # 如果有新交易，更新文件
            if new_transactions or force_full_update:
                # 读取现有数据（如果存在）
                address_filename = f"{address}.json"
                address_filepath = os.path.join(self.data_dir, address_filename)
                existing_data = {}
                
                if os.path.exists(address_filepath) and not force_full_update:
                    try:
                        with open(address_filepath, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                    except Exception as e:
                        print(f"读取现有数据时出错: {e}")
                
                # 合并新旧交易记录
                all_transactions = new_transactions + existing_data.get("交易记录", [])
                
                # 更新地址数据
                address_data = {
                    "地址": address,
                    "余额": balance,
                    "更新时间": current_time,
                    "交易记录": all_transactions
                }
                
                # 保存更新后的数据
                with open(address_filepath, 'w', encoding='utf-8') as f:
                    json.dump(address_data, f, ensure_ascii=False, indent=2)
                
                # 更新状态
                latest_signature = self.get_latest_signature(address)
                state["addresses"][address] = {
                    "last_signature": latest_signature,
                    "last_update": current_time
                }
            
            # 更新索引数据
            index_data["地址列表"].append({
                "地址": address,
                "余额": balance,
                "文件名": f"{address}.json"
            })
        
        # 保存索引文件
        with open(os.path.join(self.data_dir, "index.json"), 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)
        
        # 更新状态文件
        state["last_update"] = current_time
        self.save_fetch_state(state)
        
        print(f"数据更新完成，时间: {current_time}")
    def save_transactions_to_json(self, holders: List[Tuple[str, float]], output_dir: str) -> None:
        """
        将持有者的交易数据保存到JSON文件
        
        Args:
            holders: 持有者列表，每个元素为 (地址, 余额) 的元组
            output_dir: 输出目录路径
        """
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 创建索引数据
        index_data = {
            "更新时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "地址列表": []
        }
        
        # 处理每个持有者
        for address, balance in holders:
            print(f"正在处理地址 {address} 的交易数据...")
            
            # 获取该地址的所有交易
            transactions = self.fetch_new_transactions(address)
            
            # 创建地址数据
            address_data = {
                "地址": address,
                "余额": balance,
                "更新时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "交易记录": transactions
            }
            
            # 保存到文件
            filename = f"{address}.json"
            file_path = os.path.join(output_dir, filename)  # 修改这里：使用 file_path
            with open(file_path, 'w', encoding='utf-8') as f:  # 修改这里：使用 file_path
                json.dump(address_data, f, ensure_ascii=False, indent=2)
            
            # 更新索引数据
            index_data["地址列表"].append({
                "地址": address,
                "余额": balance,
                "文件名": filename
            })
            
            print(f"已保存地址 {address} 的交易数据")
        
        # 保存索引文件
        index_file_path = os.path.join(output_dir, "index.json")
        with open(index_file_path, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)
        
        print("所有交易数据保存完成")