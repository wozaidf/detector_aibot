"""pytest 根配置。

放在仓库根目录，pytest 会把仓库根加入 sys.path，
这样测试里可以直接 `from ai_bot.nodes import ...` 与 `from tests.test_state import ...`，
不依赖 ai_bot 是否已经安装进当前虚拟环境。
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

for _p in (ROOT, ROOT / "src"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))