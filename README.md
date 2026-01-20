# Hive to BigQuery SQL Converter

使用 LangGraph 构建的 Hive SQL 到 BigQuery SQL 转换服务，支持 Google Gemini 和 OpenAI 两种 LLM 提供商。

## 功能特性

- **Hive SQL 验证**: 使用 LLM 验证输入的 Hive SQL 语法是否正确
- **智能转换**: 将 Hive SQL 转换为 BigQuery SQL，处理函数、数据类型和语法差异
- **可配置的 BigQuery 验证**: 支持两种验证模式
  - **Dry Run 模式**: 使用 BigQuery API 进行真实验证
  - **LLM 模式**: 使用 LLM 提示词进行语法校验（无需 GCP 配置）
- **自动修正**: 根据验证错误信息自动迭代修正 SQL（最多 3 次）

## 项目结构

```
trans/
├── requirements.txt          # 依赖管理
├── env.example              # 环境变量示例
├── src/
│   ├── __init__.py
│   ├── main.py              # FastAPI 入口
│   ├── agent/
│   │   ├── __init__.py
│   │   ├── graph.py         # LangGraph 工作流定义
│   │   ├── state.py         # Agent 状态定义
│   │   └── nodes.py         # 各节点实现
│   ├── services/
│   │   ├── __init__.py
│   │   ├── bigquery.py      # BigQuery Dry Run 服务
│   │   ├── llm.py           # LLM 服务（支持 Gemini/OpenAI）
│   │   └── validation.py    # BigQuery 验证服务（支持 dry_run/llm 模式）
│   ├── prompts/
│   │   ├── __init__.py
│   │   └── templates.py     # Prompt 模板
│   └── schemas/
│       ├── __init__.py
│       └── models.py        # Pydantic 模型
└── README.md
```

## 安装

1. 创建虚拟环境并激活：

```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
# 或
.\venv\Scripts\activate  # Windows
```

2. 安装依赖：

```bash
pip install -r requirements.txt
```

3. 配置环境变量：

```bash
cp env.example .env
# 编辑 .env 文件，填入你的配置
```

## 环境变量

### LLM 配置

| 变量名 | 说明 |
|--------|------|
| `LLM_PROVIDER` | LLM 提供商，可选 `gemini` 或 `openai`（默认：gemini） |
| `GOOGLE_API_KEY` | Google Gemini API Key（当 LLM_PROVIDER=gemini 时必需） |
| `GEMINI_MODEL` | Gemini 模型名称（默认：gemini-1.5-flash） |
| `OPENAI_API_KEY` | OpenAI API Key（当 LLM_PROVIDER=openai 时必需） |
| `OPENAI_MODEL` | OpenAI 模型名称（默认：gpt-4o） |
| `OPENAI_API_BASE` | OpenAI API 基础 URL（可选，用于第三方兼容服务） |

### BigQuery 验证配置

| 变量名 | 说明 |
|--------|------|
| `BQ_VALIDATION_MODE` | BigQuery 验证模式，可选 `dry_run` 或 `llm`（默认：dry_run） |
| `GOOGLE_PROJECT_ID` | GCP 项目 ID（当 BQ_VALIDATION_MODE=dry_run 时必需） |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCP 服务账号凭证 JSON 文件路径（当 BQ_VALIDATION_MODE=dry_run 时必需） |

## 运行服务

```bash
# 开发模式（自动重载）
python -m src.main

# 或使用 uvicorn
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

服务启动后访问：
- API 文档: http://localhost:8000/docs
- 健康检查: http://localhost:8000/health

## API 使用

### 转换 SQL

**请求:**

```bash
curl -X POST http://localhost:8000/convert \
  -H "Content-Type: application/json" \
  -d '{
    "hive_sql": "SELECT date_format(dt, \"yyyy-MM-dd\") as formatted_date, collect_list(name) as names FROM my_table GROUP BY dt"
  }'
```

**响应:**

```json
{
  "success": true,
  "hive_sql": "SELECT date_format(dt, \"yyyy-MM-dd\") as formatted_date, collect_list(name) as names FROM my_table GROUP BY dt",
  "hive_valid": true,
  "hive_error": null,
  "bigquery_sql": "SELECT FORMAT_DATE('%Y-%m-%d', dt) as formatted_date, ARRAY_AGG(name) as names FROM my_table GROUP BY dt",
  "validation_success": true,
  "validation_error": null,
  "validation_mode": "dry_run",
  "retry_count": 0,
  "conversion_history": [...],
  "warning": null
}
```

## 工作流程

```
Input Hive SQL
      │
      ▼
┌─────────────────┐
│ Validate Hive   │──── Invalid ────► Return Error
│     SQL         │
└────────┬────────┘
         │ Valid
         ▼
┌─────────────────┐
│ Convert to      │
│ BigQuery SQL    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Validate BQ SQL │──── Success ────► Return Result
│ (dry_run/llm)   │
└────────┬────────┘
         │ Failed
         ▼
┌─────────────────┐
│ Retry < 3?      │──── No ────► Return with Warning
└────────┬────────┘
         │ Yes
         ▼
┌─────────────────┐
│ Fix SQL with    │
│ Error Info      │
└────────┬────────┘
         │
         └──────────► Back to Validate
```

### 验证模式说明

- **dry_run 模式**: 调用 BigQuery API 的 dry run 功能进行真实验证，需要配置 GCP 凭证
- **llm 模式**: 使用 LLM 进行语法校验，无需 GCP 配置，适合开发测试或无 GCP 环境的场景

## 支持的转换规则

### 数据类型映射

| Hive | BigQuery |
|------|----------|
| STRING | STRING |
| INT | INT64 |
| BIGINT | INT64 |
| FLOAT | FLOAT64 |
| DOUBLE | FLOAT64 |
| BOOLEAN | BOOL |
| TIMESTAMP | TIMESTAMP |
| DATE | DATE |
| ARRAY<T> | ARRAY<T> |

### 常用函数转换

| Hive | BigQuery |
|------|----------|
| `date_format(date, 'yyyy-MM-dd')` | `FORMAT_DATE('%Y-%m-%d', date)` |
| `datediff(end, start)` | `DATE_DIFF(end, start, DAY)` |
| `from_unixtime(ts)` | `TIMESTAMP_SECONDS(ts)` |
| `collect_list(col)` | `ARRAY_AGG(col)` |
| `collect_set(col)` | `ARRAY_AGG(DISTINCT col)` |
| `nvl(a, b)` | `IFNULL(a, b)` |
| `lateral view explode(arr)` | `CROSS JOIN UNNEST(arr)` |

## License

MIT
