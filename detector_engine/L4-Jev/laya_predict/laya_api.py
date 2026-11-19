from pathlib import Path
from config import get_settings

settings = get_settings()

from laya import Router

# 获取当前文件所在目录，docker-compose.yml 文件中使用了 volumes 挂载模型文件夹到容器中，确保模型文件夹在容器中可访问
BASE_DIR = Path(__file__).resolve().parent

# 1、加载模型文件夹
router = Router(
    models={
        "laya": str(BASE_DIR / "models" / "laya"),  # 英文文本、护栏、邮件分类
        "laya-multi": str(BASE_DIR / "models" / "multilingual"),  # 100+ 种语言，~2.2x 更快
        "laya-td": str(BASE_DIR / "models" / "typed-decisions"),  # 四个类型化决策工作流
        "fineTuning": str(BASE_DIR / "models" / "fine-tuning"),  # 四个类型化决策工作流
        "model-11-5": str(BASE_DIR / "models" / "model-11-5"),  # 微调后的模型
        "out5-multi": str(BASE_DIR / "models" / "out5-multi")  # 微调后的模型
    },
    device="cpu"
)

# 2、使用 laya_decide 函数调用 Laya 模型进行决策
def laya_decide(state):
    questions = {
        "needs_diagnosis": {
            "type": "noul",
            "instructions": "根据异常指标、检测证据和上游处理状态，判断该事件是否是异常事件，是否需要进一步诊断和排查。",
        }
    }
    res_multi = router.predict(state, questions, model=settings.LAYA_MODEL_NAME, max_len=8192)
    return res_multi