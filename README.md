# solana-token-tracker

src/config.py
```
import os
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 基础配置
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
REPORTS_DIR = DATA_DIR / 'reports'

TOKEN_MINT = "HhUVkZ1qz8vfMqZDemLyxBFxrHFKVSYAk7a6227Lpump"
RPC_URL = "" #  Solana RPC 节点（如 QuickNode、Helius 或其他服务）


# 创建必要的目录
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

```

python main.py