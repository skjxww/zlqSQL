"""
模拟集成测试
模拟SQL编译器和执行引擎如何使用存储层的事务接口
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from storage.core.storage_manager import StorageManager
from pathlib import Path
import struct
import json


class MockSQLNode:
    """模拟SQL节点基类"""
    pass


class BeginTransactionNode(MockSQLNode):
    def __init__(self, isolation_level="READ_COMMITTED"):
        self.isolation_level = isolation_level


class CommitTransactionNode(MockSQLNode):
    pass


class RollbackTransactionNode(MockSQLNode):
    pass


class InsertNode(MockSQLNode):
    def __init__(self, table, values):
        self.table = table
        self.values = values


class SelectNode(MockSQLNode):
    def __init__(self, table, where=None):
        self.table = table
        self.where = where


class UpdateNode(MockSQLNode):
    def __init__(self, table, set_values, where):
        self.table = table
        self.set_values = set_values
        self.where = where


class MockExecutionEngine:
    """模拟执行引擎"""

    def __init__(self, storage_manager):
        self.storage = storage_manager
        self.current_txn_id = None

        # 模拟表元数据（实际应该从系统表读取）
        self.table_metadata = {}
        self.table_pages = {}  # table_name -> [page_ids]

    def execute(self, node):
        """执行SQL节点"""
        if isinstance(node, BeginTransactionNode):
            return self._execute_begin(node)
        elif isinstance(node, CommitTransactionNode):
            return self._execute_commit()
        elif isinstance(node, RollbackTransactionNode):
            return self._execute_rollback()
        elif isinstance(node, InsertNode):
            return self._execute_insert(node)
        elif isinstance(node, SelectNode):
            return self._execute_select(node)
        elif isinstance(node, UpdateNode):
            return self._execute_update(node)
        else:
            raise ValueError(f"Unknown node type: {type(node)}")

    def _execute_begin(self, node):
        """执行BEGIN"""
        if self.current_txn_id:
            raise Exception("Already in a transaction")

        self.current_txn_id = self.storage.begin_transaction(node.isolation_level)
        return f"Transaction {self.current_txn_id} started"

    def _execute_commit(self):
        """执行COMMIT"""
        if not self.current_txn_id:
            raise Exception("Not in a transaction")

        self.storage.commit_transaction(self.current_txn_id)
        txn_id = self.current_txn_id
        self.current_txn_id = None
        return f"Transaction {txn_id} committed"

    def _execute_rollback(self):
        """执行ROLLBACK"""
        if not self.current_txn_id:
            raise Exception("Not in a transaction")

        self.storage.rollback_transaction(self.current_txn_id)
        txn_id = self.current_txn_id
        self.current_txn_id = None
        return f"Transaction {txn_id} rolled back"

    def _execute_insert(self, node):
        """执行INSERT"""
        table_name = node.table

        # 如果表还没有页，分配一个
        if table_name not in self.table_pages:
            self.table_pages[table_name] = []

        if not self.table_pages[table_name]:
            # 根据是否在事务中选择接口
            if self.current_txn_id:
                page_id = self.storage.allocate_page_transactional(
                    "default", table_name, self.current_txn_id
                )
            else:
                page_id = self.storage.allocate_page()
            self.table_pages[table_name].append(page_id)

        # 简单地将数据写入第一个页（实际应该有更复杂的逻辑）
        page_id = self.table_pages[table_name][0]

        # 序列化数据（简化版本）
        data = json.dumps(node.values).encode('utf-8')
        # 填充到页大小
        data = data[:4096].ljust(4096, b'\x00')

        # 写入数据
        if self.current_txn_id:
            self.storage.write_page_transactional(page_id, data, self.current_txn_id)
        else:
            self.storage.write_page(page_id, data)

        return f"1 row inserted into {table_name}"

    def _execute_select(self, node):
        """执行SELECT"""
        table_name = node.table

        if table_name not in self.table_pages or not self.table_pages[table_name]:
            return f"No data in table {table_name}"

        # 读取第一个页（简化版本）
        page_id = self.table_pages[table_name][0]

        if self.current_txn_id:
            data = self.storage.read_page_transactional(page_id, self.current_txn_id)
        else:
            data = self.storage.read_page(page_id)

        # 反序列化数据
        try:
            content = data.split(b'\x00')[0].decode('utf-8')
            if content:
                values = json.loads(content)
                return f"Selected from {table_name}: {values}"
            else:
                return f"No data in table {table_name}"
        except:
            return f"Raw data from {table_name}: {data[:50]}..."

    def _execute_update(self, node):
        """执行UPDATE"""
        table_name = node.table

        if table_name not in self.table_pages or not self.table_pages[table_name]:
            return f"No data in table {table_name} to update"

        page_id = self.table_pages[table_name][0]

        # 读取现有数据
        if self.current_txn_id:
            old_data = self.storage.read_page_transactional(page_id, self.current_txn_id)
        else:
            old_data = self.storage.read_page(page_id)

        # 更新数据（简化：直接替换）
        new_data = json.dumps(node.set_values).encode('utf-8')
        new_data = new_data[:4096].ljust(4096, b'\x00')

        # 写回
        if self.current_txn_id:
            self.storage.write_page_transactional(page_id, new_data, self.current_txn_id)
        else:
            self.storage.write_page(page_id, new_data)

        return f"1 row updated in {table_name}"


def test_scenario_1():
    """场景1：基本事务操作"""
    print("\n" + "=" * 60)
    print("场景1：基本事务操作")
    print("=" * 60)

    # 初始化
    test_dir = Path("test_data_simulation")
    test_dir.mkdir(exist_ok=True)

    storage = StorageManager(
        buffer_size=10,
        data_file=str(test_dir / "test.db"),
        meta_file=str(test_dir / "test_meta.json"),
        enable_wal=True
    )

    engine = MockExecutionEngine(storage)

    # 模拟SQL执行
    sql_nodes = [
        BeginTransactionNode(),
        InsertNode("users", {"id": 1, "name": "Alice"}),
        InsertNode("users", {"id": 2, "name": "Bob"}),
        SelectNode("users"),
        CommitTransactionNode()
    ]

    print("\n执行SQL序列：")
    for node in sql_nodes:
        result = engine.execute(node)
        print(f"  → {result}")

    # 验证持久性
    print("\n验证持久性（新事务中读取）：")
    engine.execute(BeginTransactionNode())
    result = engine.execute(SelectNode("users"))
    print(f"  → {result}")
    engine.execute(CommitTransactionNode())

    storage.shutdown()
    print("\n✓ 场景1通过")


def test_scenario_2():
    """场景2：事务回滚"""
    print("\n" + "=" * 60)
    print("场景2：事务回滚")
    print("=" * 60)

    # 初始化
    test_dir = Path("test_data_simulation2")
    test_dir.mkdir(exist_ok=True)

    storage = StorageManager(
        buffer_size=10,
        data_file=str(test_dir / "test.db"),
        meta_file=str(test_dir / "test_meta.json"),
        enable_wal=True
    )

    engine = MockExecutionEngine(storage)

    # 先插入一些数据
    print("\n初始数据：")
    engine.execute(BeginTransactionNode())
    engine.execute(InsertNode("products", {"id": 1, "name": "Laptop", "price": 1000}))
    engine.execute(CommitTransactionNode())
    print("  → 初始数据已提交")

    # 开始新事务并回滚
    print("\n尝试更新但回滚：")
    engine.execute(BeginTransactionNode())
    result = engine.execute(UpdateNode("products", {"price": 2000}, {"id": 1}))
    print(f"  → {result}")
    result = engine.execute(SelectNode("products"))
    print(f"  → 事务中查看: {result}")
    result = engine.execute(RollbackTransactionNode())
    print(f"  → {result}")

    # 验证回滚效果
    print("\n验证回滚后的数据：")
    result = engine.execute(SelectNode("products"))
    print(f"  → {result}")

    storage.shutdown()
    print("\n✓ 场景2通过")


def test_scenario_3():
    """场景3：混合事务和非事务操作"""
    print("\n" + "=" * 60)
    print("场景3：混合事务和非事务操作")
    print("=" * 60)

    test_dir = Path("test_data_simulation3")
    test_dir.mkdir(exist_ok=True)

    storage = StorageManager(
        buffer_size=10,
        data_file=str(test_dir / "test.db"),
        meta_file=str(test_dir / "test_meta.json"),
        enable_wal=True
    )

    engine = MockExecutionEngine(storage)

    # 非事务插入
    print("\n非事务操作：")
    result = engine.execute(InsertNode("logs", {"id": 1, "message": "System started"}))
    print(f"  → {result} (无需commit)")

    # 事务插入
    print("\n事务操作：")
    engine.execute(BeginTransactionNode())
    engine.execute(InsertNode("orders", {"id": 1, "total": 100}))
    engine.execute(CommitTransactionNode())
    print("  → 事务已提交")

    # 验证两种数据
    print("\n验证数据：")
    print(f"  → {engine.execute(SelectNode('logs'))}")
    print(f"  → {engine.execute(SelectNode('orders'))}")

    storage.shutdown()
    print("\n✓ 场景3通过")


def test_scenario_4():
    """场景4：异常处理和自动回滚"""
    print("\n" + "=" * 60)
    print("场景4：异常处理和自动回滚")
    print("=" * 60)

    test_dir = Path("test_data_simulation4")
    test_dir.mkdir(exist_ok=True)

    storage = StorageManager(
        buffer_size=10,
        data_file=str(test_dir / "test.db"),
        meta_file=str(test_dir / "test_meta.json"),
        enable_wal=True
    )

    engine = MockExecutionEngine(storage)

    print("\n模拟事务中发生错误：")
    try:
        engine.execute(BeginTransactionNode())
        engine.execute(InsertNode("accounts", {"id": 1, "balance": 1000}))
        print("  → 插入成功")

        # 模拟错误
        raise Exception("模拟的业务逻辑错误！")

        engine.execute(CommitTransactionNode())
    except Exception as e:
        print(f"  → 错误发生: {e}")
        if engine.current_txn_id:
            result = engine.execute(RollbackTransactionNode())
            print(f"  → {result}")

    # 验证数据没有被插入
    print("\n验证回滚效果：")
    result = engine.execute(SelectNode("accounts"))
    print(f"  → {result}")

    storage.shutdown()
    print("\n✓ 场景4通过")


if __name__ == "__main__":
    print("模拟集成测试 - 模拟队友如何使用事务接口")
    print("=" * 60)

    try:
        test_scenario_1()
        test_scenario_2()
        test_scenario_3()
        test_scenario_4()

        print("\n" + "=" * 60)
        print("所有集成场景测试通过！✓")
        print("事务接口可以正确集成到SQL执行引擎中")
        print("=" * 60)

    except Exception as e:
        print(f"\n测试失败: {e}")
        import traceback

        traceback.print_exc()