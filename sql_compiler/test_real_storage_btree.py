from sql_compiler.btree.BPlusTreeIndex import BPlusTreeIndex

def test_real_storage_btree():
    """测试使用真实存储管理器的B+树"""

    print("=== 真实存储版B+树测试 ===")

    # 创建多个索引测试并发
    indexes = {}

    for i in range(3):
        index_name = f"test_index_{i}"
        btree = BPlusTreeIndex(index_name, order=50)
        indexes[index_name] = btree

        # 插入测试数据
        test_data = [(j + i * 100, f"record_{i}_{j}") for j in range(1, 21)]

        print(f"\n索引 {index_name} 插入测试:")
        success_count = 0
        for key, value in test_data:
            if btree.insert(key, value):
                success_count += 1

        print(f"  成功插入: {success_count}/{len(test_data)}")

        # 查找测试
        print(f"  查找测试:")
        found_count = 0
        for key, expected in test_data[:5]:  # 只测试前5个
            result = btree.search(key)
            if result:
                found_count += 1
                print(f"    {key} -> {result}")
        print(f"  成功查找: {found_count}/5")

    # 统计信息
    print(f"\n=== 索引统计信息 ===")
    for name, btree in indexes.items():
        stats = btree.get_statistics()
        print(f"\n{name}:")
        print(f"  实现方式: {stats['implementation']}")
        print(f"  高度: {stats.get('height', 'N/A')}")
        print(f"  节点数: {stats.get('node_count', 'N/A')}")

        if 'storage_stats' in stats:
            storage_stats = stats['storage_stats']
            print(f"  缓存命中: {storage_stats.get('buffer_hits', 0)}")
            print(f"  缓存未命中: {storage_stats.get('buffer_misses', 0)}")
            print(f"  刷盘次数: {storage_stats.get('flush_count', 0)}")

    # 范围查询测试
    print(f"\n=== 范围查询测试 ===")
    btree = indexes["test_index_0"]
    range_results = btree.range_search(5, 15)
    print(f"范围查询 [5, 15]: 找到 {len(range_results)} 条记录")
    for key, value in range_results[:3]:  # 只显示前3条
        print(f"  {key} -> {value}")

    # 性能测试
    print(f"\n=== 性能测试 ===")
    btree = BPlusTreeIndex("performance_test", order=100)

    import time
    start_time = time.time()

    # 插入大量数据
    insert_count = 1000
    success_count = 0
    for i in range(insert_count):
        if btree.insert(i, f"perf_record_{i}"):
            success_count += 1

    insert_time = time.time() - start_time
    print(f"插入 {insert_count} 条记录:")
    print(f"  成功: {success_count}")
    print(f"  耗时: {insert_time:.3f}秒")
    print(f"  吞吐量: {success_count / insert_time:.1f} 插入/秒")

    # 查找性能
    start_time = time.time()
    found_count = 0
    search_count = 100

    for i in range(0, insert_count, insert_count // search_count):
        if btree.search(i):
            found_count += 1

    search_time = time.time() - start_time
    print(f"随机查找 {search_count} 次:")
    print(f"  找到: {found_count}")
    print(f"  耗时: {search_time:.3f}秒")
    print(f"  吞吐量: {search_count / search_time:.1f} 查找/秒")

    # 最终统计
    final_stats = btree.get_statistics()
    print(f"\n性能测试索引最终统计:")
    print(f"  高度: {final_stats.get('height')}")
    print(f"  节点数: {final_stats.get('node_count')}")

    # 清理
    print(f"\n=== 清理资源 ===")
    for btree in indexes.values():
        btree.close()
    btree.close()

    print("测试完成")


if __name__ == "__main__":
    test_real_storage_btree()