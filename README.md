# ZLQ SQL 数据库管理系统

一个功能完整的SQL数据库管理系统，支持完整的SQL语法、图形化界面、AI辅助查询和高级优化功能。

## 🚀 项目特色

- **完整的SQL支持**: 支持DDL、DML、DQL等完整的SQL操作
- **图形化界面**: 提供现代化的GUI界面，支持SQL查询、自然语言查询和可视化
- **AI智能助手**: 集成自然语言转SQL功能，支持智能查询建议
- **高级优化器**: 内置查询优化器，支持执行计划可视化
- **存储引擎**: 自研存储引擎，支持B+树索引、WAL日志等
- **事务支持**: 完整的事务管理，支持ACID特性
- **多表空间**: 支持系统表空间、用户表空间、临时表空间等

## 📁 项目结构

```
zlqSQL/
├── cli/                    # 命令行界面
│   ├── main.py            # CLI主程序
│   └── storage_monitor_gui.py  # 存储监控界面
├── gui/                   # 图形化界面
│   ├── gui_main.py        # GUI主程序
│   ├── gui_components/    # GUI组件
│   └── core/              # 核心功能模块
├── sql_compiler/          # SQL编译器
│   ├── lexer/             # 词法分析器
│   ├── parser/            # 语法分析器
│   ├── semantic/          # 语义分析器
│   ├── codegen/           # 代码生成器
│   ├── optimizer/         # 查询优化器
│   └── diagnostics/       # 错误诊断
├── storage/               # 存储引擎
│   ├── core/              # 核心存储组件
│   └── utils/             # 存储工具
├── engine/                # 执行引擎
├── catalog/               # 目录管理
├── extensions/            # 扩展功能
│   ├── enhanced_nl2sql.py # 自然语言转SQL
│   ├── plan_visualizer.py # 执行计划可视化
│   └── smart_completion.py # 智能补全
└── data/                  # 数据目录
```

## 🛠️ 安装与运行

### 环境要求

- Python 3.8+
- 推荐使用虚拟环境

### 安装依赖

```bash
pip install -r requirements.txt
```

### 运行方式

#### 1. 图形化界面（推荐）

```bash
python start_gui.py
```

#### 2. 命令行界面

```bash
python cli/main.py
```

#### 3. 运行测试

```bash
# 运行完整测试套件
python run.py

# 运行特定测试
python run.py --basic-test      # 基础测试
python run.py --complex-test    # 复杂查询测试
python run.py --perf-test       # 性能测试
```

## 💡 主要功能

### SQL支持

- **DDL操作**: CREATE TABLE, DROP TABLE, ALTER TABLE
- **DML操作**: INSERT, UPDATE, DELETE
- **DQL操作**: SELECT with JOIN, WHERE, GROUP BY, ORDER BY
- **索引管理**: CREATE INDEX, DROP INDEX
- **视图管理**: CREATE VIEW, DROP VIEW
- **事务管理**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT

### 高级功能

- **查询优化**: 基于成本的查询优化器
- **执行计划可视化**: 图形化显示查询执行计划
- **自然语言查询**: AI驱动的自然语言转SQL
- **智能补全**: 基于上下文的SQL智能补全
- **错误诊断**: 智能SQL错误分析和建议

### 存储特性

- **B+树索引**: 高效的索引结构
- **WAL日志**: 预写日志保证数据一致性
- **缓冲池**: 内存缓冲管理
- **多表空间**: 支持不同类型的表空间
- **并发控制**: 支持多用户并发访问

## 🎯 使用示例

### 基础SQL操作

```sql
-- 创建表
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100)
);

-- 插入数据
INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com');

-- 查询数据
SELECT * FROM users WHERE name = '张三';

-- 创建索引
CREATE INDEX idx_name ON users(name);
```

### 复杂查询

```sql
-- 多表连接查询
SELECT u.name, o.amount 
FROM users u 
JOIN orders o ON u.id = o.user_id 
WHERE o.amount > 1000;

-- 聚合查询
SELECT department, COUNT(*) as employee_count
FROM employees 
GROUP BY department 
HAVING COUNT(*) > 5;
```

### 自然语言查询

在GUI界面中，你可以使用自然语言进行查询：

- "显示所有用户的信息"
- "查找金额大于1000的订单"
- "统计每个部门的员工数量"

## 🔧 配置说明

### 环境变量

```bash
# DeepSeek API配置（用于自然语言查询）
export DEEPSEEK_API_KEY="your_api_key_here"
```

### 存储配置

系统支持多种存储配置选项：

- 缓冲池大小
- WAL日志启用/禁用
- 并发控制级别
- 表空间配置

## 📊 性能特性

- **查询优化**: 基于统计信息的成本估算
- **索引支持**: B+树索引加速查询
- **内存管理**: 智能缓冲池管理
- **并发处理**: 支持多用户并发访问
- **事务隔离**: 支持不同级别的事务隔离

## 🧪 测试

项目包含完整的测试套件：

- **单元测试**: 各模块的单元测试
- **集成测试**: 端到端功能测试
- **性能测试**: 查询性能基准测试
- **压力测试**: 并发和负载测试

运行测试：

```bash
python run.py --all-test
```

## 🤝 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 📝 开发计划

- [ ] 支持更多SQL函数
- [ ] 分布式查询支持
- [ ] 更多存储引擎选项
- [ ] 性能监控面板
- [ ] 数据导入/导出工具

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 👥 作者

- **ZLQ** - 项目主要开发者

## 🙏 致谢

感谢所有为这个项目做出贡献的开发者和测试人员。

---

**注意**: 这是一个教学和研究项目，不建议在生产环境中使用。
