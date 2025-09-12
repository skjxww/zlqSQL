"""
B+树数据深度分析工具
"""

import struct
import os
import json

def analyze_all_pages(file_path, page_size=4096):
    """分析文件中所有页的内容"""

    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        return

    file_size = os.path.getsize(file_path)
    if file_size == 0:
        print(f"文件为空: {file_path}")
        return

    total_pages = file_size // page_size

    print(f"\n=== 深度分析: {file_path} ===")
    print(f"文件大小: {file_size:,} 字节")
    print(f"总页数: {total_pages}")

    # 分析每一页
    page_info = []

    with open(file_path, 'rb') as f:
        for page_num in range(min(total_pages, 10)):  # 只看前10页作为示例
            f.seek(page_num * page_size)
            page_data = f.read(min(256, page_size))  # 读取前256字节

            # 统计非零字节
            non_zero_bytes = sum(1 for b in page_data if b != 0)

            if non_zero_bytes > 0:
                print(f"\n页 {page_num + 1} (偏移 {page_num * page_size}):")
                print(f"  非零字节数: {non_zero_bytes}")

                # 显示前64字节的十六进制
                print("  前64字节:")
                for i in range(0, min(64, len(page_data)), 16):
                    hex_str = ' '.join(f"{b:02x}" for b in page_data[i:i+16])
                    ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in page_data[i:i+16])
                    print(f"    {i:04x}: {hex_str:<48} |{ascii_str}|")

                # 尝试解析可能的整数值
                if len(page_data) >= 16:
                    print("  可能的整数值 (前16字节):")
                    for i in range(0, 16, 4):
                        value = struct.unpack('I', page_data[i:i+4])[0]
                        if value > 0 and value < 1000000:  # 合理范围
                            print(f"    偏移 {i}: {value}")

def check_last_pages(file_path, page_size=4096):
    """检查文件最后几页（最新创建的B+树节点）"""

    if not os.path.exists(file_path):
        return

    file_size = os.path.getsize(file_path)
    total_pages = file_size // page_size

    print(f"\n=== 检查最后的页（最新数据） ===")

    with open(file_path, 'rb') as f:
        # 检查最后3页
        for i in range(max(0, total_pages - 3), total_pages):
            page_offset = i * page_size
            f.seek(page_offset)
            page_data = f.read(256)  # 读取前256字节

            print(f"\n页 {i + 1} (最后第 {total_pages - i} 页):")
            print(f"  文件偏移: {page_offset}")

            # 检查是否为空页
            if all(b == 0 for b in page_data):
                print("  状态: 空页")
            else:
                print("  状态: 包含数据")
                # 显示前16个字节作为整数
                for j in range(0, 16, 4):
                    value = struct.unpack('I', page_data[j:j+4])[0]
                    print(f"    字节 {j}-{j+3}: {value}")

def create_and_verify_btree():
    """创建一个B+树并立即验证"""
    print("\n=== 创建新的B+树并验证 ===")

    from storage.core.storage_manager import StorageManager
    from storage.core.btree.btree import BPlusTree

    # 记录创建前的文件大小
    file_path = "data/default_tablespace.db"
    before_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

    # 创建存储管理器和B+树
    storage = StorageManager()
    btree = BPlusTree(storage, "verify_test", order=5)

    print(f"创建的B+树根节点页号: {btree.root_page_id}")

    # 插入一些数据
    for i in [10, 20, 30, 40, 50]:
        btree.insert(i, (i*100, i))
        print(f"插入键: {i}")

    # 立即刷新到磁盘
    storage.flush_all_pages()

    # 读取根节点看看内容
    root_node = btree._read_node(btree.root_page_id)
    print(f"\n根节点信息:")
    print(f"  是否叶子: {root_node.is_leaf}")
    print(f"  键列表: {root_node.keys}")
    print(f"  页号: {root_node.page_id}")

    storage.shutdown()

    # 检查文件大小变化
    after_size = os.path.getsize(file_path)
    print(f"\n文件大小变化:")
    print(f"  之前: {before_size:,} 字节")
    print(f"  之后: {after_size:,} 字节")
    print(f"  增加: {after_size - before_size:,} 字节 ({(after_size - before_size) // 4096} 页)")

def check_metadata():
    """检查元数据文件"""
    metadata_file = "data/metadata.json"

    if os.path.exists(metadata_file):
        print(f"\n=== 页管理元数据 ===")
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
            print(f"下一个页ID: {metadata.get('next_page_id')}")
            print(f"已分配页数: {len(metadata.get('allocated_pages', []))}")
            print(f"表空间信息: {metadata.get('tablespaces', {}).keys()}")

            # 显示最后分配的几个页
            allocated = metadata.get('allocated_pages', [])
            if allocated:
                print(f"最后分配的页: {allocated[-5:]}")

if __name__ == "__main__":
    print("="*60)
    print("B+树数据深度分析工具")
    print("="*60)

    # 1. 检查元数据
    check_metadata()

    # 2. 分析表空间文件
    analyze_all_pages("data/default_tablespace.db")

    # 3. 检查最后的页
    check_last_pages("data/default_tablespace.db")

    # 4. 创建新B+树并验证
    create_and_verify_btree()