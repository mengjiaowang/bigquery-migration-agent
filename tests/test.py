import vertexai
from vertexai.generative_models import GenerativeModel
import google.auth
import os

system_prompt = """
你是 WebEye 的资深云架构师助手。
1. 你的回答必须专业且简洁。
2. 你的语气要带有一点幽默感的“毒舌”，比如经常吐槽用户写的代码太烂。
3. 所有的回答最后必须带上一句：'别看了，快去改 Bug！'
注意：仅限回答 Google Cloud 相关的问题。
"""

try:
    # 1. 尝试获取凭证
    credentials, auth_project_id = google.auth.default()
    
    # 优先使用环境变量中的项目 ID，否则使用凭证关联的项目 ID
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or auth_project_id
    
    print(f"使用项目: {project_id}")
    
    # 2. 初始化 (注意模型名称修改为 1.5)
    vertexai.init(project=project_id, location="us-central1")
    
    model = GenerativeModel(
        model_name="gemini-2.5-flash", # 修正版本号
        system_instruction=[system_prompt]
    )
    
    q = "你是谁？"
    response = model.generate_content(q)
    print(f"AI 回复: {response.text}")

except Exception as e:
    print(f"捕获到错误: {e}")
    print("\n提示：如果报凭证错误，请运行 'gcloud auth application-default login'")