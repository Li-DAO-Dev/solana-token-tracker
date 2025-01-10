import requests
import base64
import json
import os
from datetime import datetime
from typing import List, Tuple, Dict, Optional, Any
import matplotlib.pyplot as plt
from markdown import markdown
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration

class TokenDataAnalyzer:
    def __init__(self, genesis_timestamp: int = 1584500000, slot_duration: float = 0.4):
        """
        初始化 TokenDataAnalyzer
        
        Args:
            genesis_timestamp: Solana 创世时间戳（默认 2020-03-16）
            slot_duration: 每个 Slot 的时间间隔（默认 0.4 秒）
        """
        self.genesis_timestamp = genesis_timestamp
        self.slot_duration = slot_duration
        self.analysis_results: Dict = {}
        
        # CSS样式配置
        self.css_style = """
            body { 
                font-family: Arial, sans-serif;
                margin: 40px;
                line-height: 1.6;
            }
            h1 { 
                color: #2c3e50;
                border-bottom: 2px solid #eee;
                padding-bottom: 10px;
            }
            h2 { 
                color: #34495e;
                margin-top: 30px;
            }
            h3 { 
                color: #7f8c8d;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                margin: 20px 0;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f5f5f5;
            }
            img {
                max-width: 100%;
                height: auto;
                margin: 20px 0;
            }
        """

    def load_transactions(self, input_dir: str) -> List[Dict]:
        """
        从目录加载所有交易数据
        
        Args:
            input_dir: 输入目录路径
            
        Returns:
            List[Dict]: 交易数据列表
        """
        all_transactions = []
        
        try:
            # 首先读取索引文件
            index_filepath = os.path.join(input_dir, "index.json")
            if not os.path.exists(index_filepath):
                raise FileNotFoundError(f"索引文件不存在: {index_filepath}")
                
            with open(index_filepath, "r", encoding="utf-8") as f:
                index_data = json.load(f)
            
            # 遍历所有地址文件
            for address_info in index_data["地址列表"]:
                address = address_info["地址"]
                address_filepath = os.path.join(input_dir, address_info["文件名"])
                
                if not os.path.exists(address_filepath):
                    print(f"警告: 地址 {address} 的数据文件不存在")
                    continue
                    
                with open(address_filepath, "r", encoding="utf-8") as f:
                    address_data = json.load(f)
                    
                # 将交易记录转换为统一格式
                for tx in address_data["交易记录"]:
                    all_transactions.append({
                        "地址": address,
                        "交易签名": tx["交易签名"],
                        "交易详情": tx["交易详情"]
                    })
                    
            return all_transactions
            
        except Exception as e:
            print(f"加载交易数据时发生错误: {e}")
            return []

    def slot_to_standard_time(self, slot: int) -> str:
        """
        将 Solana slot 转换为标准时间
        
        Args:
            slot: Solana slot 编号
            
        Returns:
            str: 格式化的时间字符串
        """
        timestamp = self.genesis_timestamp + (slot * self.slot_duration)
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    def analyze_transactions(self, transactions: List[Dict]) -> None:
        """
        分析交易数据，提取持仓变化
        
        Args:
            transactions: 交易记录列表
        """
        self.analysis_results = {}
        
        for tx in transactions:
            address = tx["地址"]
            transaction_details = tx["交易详情"]
            meta = transaction_details.get("meta", {})
            post_token_balances = meta.get("postTokenBalances", [])
            pre_token_balances = meta.get("preTokenBalances", [])

            # 提取 token 转移前后的余额
            if post_token_balances and pre_token_balances:
                try:
                    pre_balance = int(pre_token_balances[0].get("uiTokenAmount", {}).get("amount", 0))
                    post_balance = int(post_token_balances[0].get("uiTokenAmount", {}).get("amount", 0))
                    balance_change = post_balance - pre_balance
                except Exception as e:
                    print(f"解析余额时发生错误: {e}")
                    continue
            else:
                continue

            # 判断交易类型
            if address not in self.analysis_results:
                self.analysis_results[address] = []

            if balance_change > 0:
                transaction_type = "加仓"
            elif balance_change < 0:
                transaction_type = "减仓"
            else:
                transaction_type = "无变化"

            # 保存分析结果
            self.analysis_results[address].append({
                "交易签名": tx["交易签名"],
                "Slot": transaction_details["slot"],
                "标准时间": self.slot_to_standard_time(transaction_details["slot"]),
                "余额变化": balance_change,
                "交易类型": transaction_type,
                "交易详情": transaction_details
            })

    def save_visualizations(self, output_dir: str) -> List[Tuple[str, str]]:
        """
        生成并保存可视化图表
        
        Args:
            output_dir: 输出目录路径
            
        Returns:
            List[Tuple[str, str]]: [(地址, 图片路径), ...]
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        image_paths = []
        
        for address, transactions in self.analysis_results.items():
            # 按时间顺序排序交易记录
            sorted_transactions = sorted(transactions, key=lambda x: x["Slot"])
            times = [tx["标准时间"] for tx in sorted_transactions]
            balance_changes = [tx["余额变化"] for tx in sorted_transactions]

            # 计算累计持仓变化
            cumulative_balance = [sum(balance_changes[:i+1]) for i in range(len(balance_changes))]

            # 绘制持仓变化曲线
            plt.figure(figsize=(10, 6))
            plt.plot(times, cumulative_balance, marker="o", label="持仓变化")
            plt.title(f"地址 {address} 的持仓变化")
            plt.xlabel("时间")
            plt.ylabel("累计余额变化")
            plt.xticks(rotation=45)
            plt.legend()
            plt.grid()

            # 保存图表为图片
            image_path = os.path.join(output_dir, f"{address}_balance.png")
            plt.savefig(image_path, bbox_inches='tight')
            plt.close()
            
            # 保存相对路径
            relative_path = os.path.relpath(image_path, output_dir)
            image_paths.append((address, relative_path))

        return image_paths

    def generate_markdown_report(self, image_paths: List[Tuple[str, str]], 
                               output_dir: str) -> str:
        """
        生成 Markdown 报告
        
        Args:
            image_paths: 图表路径列表 [(地址, 图片路径), ...]
            output_dir: 输出目录路径
            
        Returns:
            str: 生成的 Markdown 内容
        """
        current_date = datetime.now()
        report_date = current_date.strftime('%Y-%m-%d')
        report_datetime = current_date.strftime('%Y%m%d_%H%M%S')
        report_lines = [
            "# 每日持仓分析报告",
            f"报告日期：{datetime.now().strftime('%Y-%m-%d')}",
            "\n## 分析概要",
            f"本次分析包含 {len(self.analysis_results)} 个地址的交易数据",
            "\n## 详细分析"
        ]

        # 遍历每个地址的分析结果
        for address, transactions in self.analysis_results.items():
            report_lines.extend([
                f"\n### 地址：{address}",
                "\n#### 持仓变化分析",
                "| 时间 | 余额变化 | 交易类型 |",
                "|------------|----------|----------|"
            ])

            # 添加交易记录
            sorted_transactions = sorted(transactions, key=lambda x: x["Slot"])
            for tx in sorted_transactions:
                report_lines.append(
                    f"| {tx['标准时间']} | {tx['余额变化']} | {tx['交易类型']} |"
                )

            # 添加图表
            image_path = next((path for addr, path in image_paths if addr == address), None)
            if image_path:
                report_lines.extend([
                    "\n#### 持仓变化图表",
                    f"![持仓变化图表]({image_path})"
                ])

            report_lines.append("\n---")

        markdown_content = "\n".join(report_lines)
        
        # 生成带时间戳的文件名
        md_filename = f"daily_report_{report_datetime}.md"
        md_file_path = os.path.join(output_dir, md_filename)
        with open(md_file_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)
            
        return markdown_content

    def convert_markdown_to_pdf(self, markdown_content: str, output_dir: str) -> None:
        """
        将 Markdown 内容转换为 PDF
        
        Args:
            markdown_content: Markdown 格式的报告内容
            output_dir: 输出目录路径
        """
        try:
            # 确保输出目录存在
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # 生成 HTML 内容
            html_content = markdown(
                markdown_content,
                extensions=['tables', 'fenced_code']
            )

            html_document = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>每日持仓分析报告</title>
                <style>
                    {self.css_style}
                </style>
            </head>
            <body>
                {html_content}
            </body>
            </html>
            """

            # 配置 WeasyPrint
            font_config = FontConfiguration()
            html = HTML(string=html_document, base_url=output_dir)
            css = CSS(string=self.css_style, font_config=font_config)
            
            # 生成 PDF 文件
            current_date = datetime.now()
            report_date = current_date.strftime('%Y-%m-%d')
            report_datetime = current_date.strftime('%Y%m%d_%H%M%S')
            pdf_filename = f"daily_report_{report_datetime}.pdf"
            output_file = os.path.join(output_dir, pdf_filename)
            html.write_pdf(
                output_file,
                stylesheets=[css],
                font_config=font_config
            )
            
            print(f"PDF报告已生成：{output_file}")
            
        except Exception as e:
            print(f"生成PDF时发生错误: {str(e)}")

    def process_data(self, input_dir: str, output_dir: str, reports_dir: str) -> None:
        """
        处理数据的主流程
        
        Args:
            input_dir: 输入目录路径
            output_dir: 输出目录路径
        """
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 加载交易数据
        transactions = self.load_transactions(input_dir)
        
        if not transactions:
            print("未找到交易记录，无法进行分析。")
            return

        print("正在分析交易记录...")
        self.analyze_transactions(transactions)

        print("正在生成持仓变化图表...")
        image_paths = self.save_visualizations(output_dir)

        print("正在生成每日报告...")
        markdown_content = self.generate_markdown_report(image_paths, output_dir)
        
        print("正在生成PDF报告...")
        self.convert_markdown_to_pdf(markdown_content, reports_dir)

class EnhancedAnalyzer(TokenDataAnalyzer):
    """
    扩展的分析器，提供更详细的市场分析功能
    """
    def analyze_market_sentiment(self) -> Dict:
        """
        分析市场情绪
        
        Returns:
            Dict: 市场情绪分析结果
        """
        total_transactions = 0
        total_increase = 0
        total_decrease = 0
        total_volume = 0
        
        # 统计各类型交易数量和交易量
        for address, transactions in self.analysis_results.items():
            for tx in transactions:
                total_transactions += 1
                balance_change = tx["余额变化"]
                total_volume += abs(balance_change)
                
                if balance_change > 0:
                    total_increase += 1
                elif balance_change < 0:
                    total_decrease += 1
        
        # 计算买卖比例
        buy_sell_ratio = total_increase / total_decrease if total_decrease > 0 else float('inf')
        
        # 判断市场情绪
        if buy_sell_ratio > 1.5:
            sentiment = "强烈看多"
        elif buy_sell_ratio > 1.2:
            sentiment = "轻度看多"
        elif buy_sell_ratio > 0.8:
            sentiment = "市场中性"
        elif buy_sell_ratio > 0.5:
            sentiment = "轻度看空"
        else:
            sentiment = "强烈看空"
            
        return {
            "总交易次数": total_transactions,
            "买入次数": total_increase,
            "卖出次数": total_decrease,
            "总交易量": total_volume,
            "买卖比例": round(buy_sell_ratio, 2),
            "市场情绪": sentiment
        }

    def analyze_address_patterns(self) -> Dict:
        """
        分析地址交易模式
        
        Returns:
            Dict: 地址交易模式分析结果
        """
        patterns = {}
        
        for address, transactions in self.analysis_results.items():
            # 计算该地址的统计数据
            total_volume = sum(abs(tx["余额变化"]) for tx in transactions)
            increase_count = sum(1 for tx in transactions if tx["余额变化"] > 0)
            decrease_count = sum(1 for tx in transactions if tx["余额变化"] < 0)
            
            # 判断交易模式
            if len(transactions) >= 10:
                frequency = "高频"
            elif len(transactions) >= 5:
                frequency = "中频"
            else:
                frequency = "低频"
                
            if increase_count > decrease_count * 2:
                behavior = "积极买入"
            elif decrease_count > increase_count * 2:
                behavior = "积极卖出"
            else:
                behavior = "中性交易"
                
            patterns[address] = {
                "交易频率": frequency,
                "交易行为": behavior,
                "总交易量": total_volume,
                "买入次数": increase_count,
                "卖出次数": decrease_count
            }
            
        return patterns

    def generate_market_analysis(self) -> str:
        """
        生成市场分析报告
        
        Returns:
            str: 市场分析文字描述
        """
        sentiment = self.analyze_market_sentiment()
        patterns = self.analyze_address_patterns()
        
        # 计算整体统计
        active_addresses = len(patterns)
        high_freq_addresses = sum(1 for p in patterns.values() if p["交易频率"] == "高频")
        buying_addresses = sum(1 for p in patterns.values() if p["交易行为"] == "积极买入")
        selling_addresses = sum(1 for p in patterns.values() if p["交易行为"] == "积极卖出")
        
        analysis = [
            "## 市场分析",
            "\n### 市场情绪指标",
            f"- 当前市场情绪: {sentiment['市场情绪']}",
            f"- 买卖比例: {sentiment['买卖比例']}",
            f"- 总交易次数: {sentiment['总交易次数']}",
            f"- 总交易量: {sentiment['总交易量']}",
            "\n### 地址行为分析",
            f"- 活跃地址数: {active_addresses}",
            f"- 高频交易地址数: {high_freq_addresses}",
            f"- 积极买入地址数: {buying_addresses}",
            f"- 积极卖出地址数: {selling_addresses}",
            "\n### 市场趋势判断"
        ]
        
        # 添加趋势判断
        if sentiment['买卖比例'] > 1.2 and buying_addresses > selling_addresses:
            analysis.append("市场整体呈现上升趋势，买入意愿强烈，建议关注短期上涨机会。")
        elif sentiment['买卖比例'] < 0.8 and selling_addresses > buying_addresses:
            analysis.append("市场整体呈现下降趋势，卖出压力较大，建议注意风险控制。")
        else:
            analysis.append("市场处于震荡整理阶段，建议等待更明确的方向性信号。")
            
        return "\n".join(analysis)
    
    def process_data(self, input_dir: str, output_dir: str, reports_dir: str) -> None:
        """
        处理数据的主流程
        
        Args:
            input_dir: 输入目录路径
            output_dir: 输出目录路径
            reports_dir: 报告输出目录路径
        """
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 加载交易数据
        transactions = self.load_transactions(input_dir)
        
        if not transactions:
            print("未找到交易记录，无法进行分析。")
            return

        print("正在分析交易记录...")
        self.analyze_transactions(transactions)

        print("正在生成持仓变化图表...")
        image_paths = self.save_visualizations(output_dir)

        print("正在生成每日报告...")
        markdown_content, _ = self.generate_markdown_report(image_paths, output_dir)
        
        print("正在生成PDF报告...")
        self.convert_markdown_to_pdf(markdown_content, reports_dir)

    
    def generate_markdown_report(self, image_paths: List[Tuple[str, str]], 
                               output_dir: str) -> Tuple[str, str]:
        """
        生成 Markdown 格式的报告
    
        Args:
            image_paths: 图表路径列表 [(地址, 图片路径), ...]
            output_dir: 输出目录路径
        
        Returns:
            Tuple[str, str]: (Markdown内容, 报告文件名)
        """
        markdown_content = ""  # 初始化变量
        md_filename = ""
    
        try:
            # 获取当前日期时间
            current_date = datetime.now()
            report_date = current_date.strftime('%Y-%m-%d')
            report_datetime = current_date.strftime('%Y%m%d_%H%M%S')
        
            # 生成市场分析
            market_sentiment = self.analyze_market_sentiment()
        
            # 生成市场分析文本
            market_analysis = "\n## 市场情绪分析\n"
            market_analysis += f"- 总交易次数：{market_sentiment['总交易次数']}\n"
            market_analysis += f"- 买入次数：{market_sentiment['买入次数']}\n"
            market_analysis += f"- 卖出次数：{market_sentiment['卖出次数']}\n"
            market_analysis += f"- 买卖比例：{market_sentiment['买卖比例']}\n"
            market_analysis += f"- 市场情绪：{market_sentiment['市场情绪']}\n"
        
            report_lines = [
                "# 每日持仓分析报告",
                f"报告日期：{report_date}",
                "\n## 分析概要",
                f"本次分析包含 {len(self.analysis_results)} 个地址的交易数据",
                market_analysis,
                "\n## 详细地址分析"
            ]

            # 遍历每个地址的分析结果
            for address, transactions in self.analysis_results.items():
                # 计算地址统计数据
                total_volume = sum(abs(tx["余额变化"]) for tx in transactions)
                buy_count = sum(1 for tx in transactions if tx["余额变化"] > 0)
                sell_count = sum(1 for tx in transactions if tx["余额变化"] < 0)
            
                report_lines.extend([
                    f"\n### 地址：{address}",
                    "\n#### 地址统计",
                    f"- 总交易次数：{len(transactions)}",
                    f"- 总交易量：{total_volume}",
                    f"- 买入次数：{buy_count}",
                    f"- 卖出次数：{sell_count}",
                    "\n#### 持仓变化记录",
                    "| 时间 | 余额变化 | 交易类型 |",
                    "|------------|----------|----------|"
                ])

            # 添加交易记录
                sorted_transactions = sorted(transactions, key=lambda x: x["Slot"])
                for tx in sorted_transactions:
                    report_lines.append(
                       f"| {tx['标准时间']} | {tx['余额变化']} | {tx['交易类型']} |"
                    )

               # 添加图表
                image_path = next((path for addr, path in image_paths if addr == address), None)
                if image_path:
                   report_lines.extend([
                        "\n#### 持仓变化图表",
                        f"![持仓变化图表]({image_path})"
                    ])

                report_lines.append("\n---")
    
            markdown_content = "\n".join(report_lines)
        
            # 生成带时间戳的文件名
            md_filename = f"daily_report_{report_datetime}.md"
            md_file_path = os.path.join(output_dir, md_filename)
        
            # 保存 Markdown 文件
            with open(md_file_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            
        except Exception as e:
            print(f"生成Markdown报告时发生错误: {str(e)}")
        
        return markdown_content, md_filename
    def convert_markdown_to_pdf(self, markdown_content: str, output_dir: str) -> None:
        """
        将 Markdown 内容转换为 PDF
        
        Args:
            markdown_content: Markdown 格式的报告内容
            output_dir: 输出目录路径
        """
        try:
            # 确保 markdown_content 是字符串
            if isinstance(markdown_content, tuple):
                markdown_content = markdown_content[0]

            # 确保输出目录存在
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # 生成 HTML 内容
            html_content = markdown(
                markdown_content,
                extensions=['tables', 'fenced_code']
            )

            html_document = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>每日持仓分析报告</title>
                <style>
                    {self.css_style}
                </style>
            </head>
            <body>
                {html_content}
            </body>
            </html>
            """

            # 配置 WeasyPrint
            font_config = FontConfiguration()
            html = HTML(string=html_document, base_url=output_dir)
            css = CSS(string=self.css_style, font_config=font_config)
            
            # 生成 PDF 文件
            current_date = datetime.now()
            report_datetime = current_date.strftime('%Y%m%d_%H%M%S')
            pdf_filename = f"daily_report_{report_datetime}.pdf"
            output_file = os.path.join(output_dir, pdf_filename)
            html.write_pdf(
                output_file,
                stylesheets=[css],
                font_config=font_config
            )
            
            print(f"PDF报告已生成：{output_file}")
            
        except Exception as e:
            print(f"生成PDF时发生错误: {str(e)}")