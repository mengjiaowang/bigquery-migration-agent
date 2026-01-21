# 📊 SQL 转换统计报告

> **生成时间**: 2026-01-20

根据 `result/` 目录下的 15 个转换结果文件统计。

## 📈 整体概览

| 指标 | 数值 | 说明 |
| :--- | :--- | :--- |
| **总文件数** | 15 | |
| **成功** | 15 | 100% |
| **失败** | 0 | |

### 类型分布
- **DDL**: 7 个 (建表/视图语句)
- **DML**: 6 个 (数据操作语句)
- **SQL**: 2 个 (通用/其他)

## 📋 详细统计表

| 文件名 | 类型 | 结果 | BigQuery 验证 | 重试次数
| :--- | :--- | :--- | :--- | :--- |
| `ddl-1-1-0120.txt` | DDL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 0 
| `ddl-1-2-0120.txt` | DDL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 0 
| `ddl-1-3-0120.txt` | DDL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 1 
| `ddl-1-4-0120.txt` | DDL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 1 
| `ddl-1-5-0120.txt` | DDL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 0 
| `ddl-1-6-0120.txt` | DDL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 0
| `ddl-alterview-0120.txt` | DDL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 0 
| `dml-1-0120.txt` | DML | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 3 
| `dml-2-0120.txt` | DML | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 0 
| `dml-3-0120.txt` | DML | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 5 
| `dml-4-0120.txt` | DML | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 3 
| `dml-5-0120.txt` | DML | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 4 
| `dml-6-0120.txt` | DML | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 0 
| `sql-01-0120.txt` | SQL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 5 
| `sql02-0120.txt` | SQL | <span style="color:green">✓ 成功</span> | <span style="color:green">✓ 通过</span> | 2 

## 💡 分析摘要

1. **高成功率**：所有 15 个任务均成功转换并通过了 BigQuery Dry Run 校验。
2. **复杂任务耗时**：`dml-3-0120.txt` 和 `dml-5-0120.txt` 重试次数较多（4-5次），因为这两个 SQL 逻辑较复杂，触发了多次自动修复。
3. **DDL 效率高**：大部分 DDL 任务在 1 分钟内完成，且重试较少。
