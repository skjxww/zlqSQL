import sys
import os

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 导入测试套件
from sql_compiler.complete_test import (
    test_basic_ddl,
    test_basic_dml,
    test_complex_queries,
    test_error_handling,
    test_advanced_features,
    test_business_scenarios,
    run_performance_test,
    main as run_all_tests
)


def run_basic_tests():
    """运行基础测试"""
    print("🧪 运行基础测试...")
    print("=" * 60)

    tests = [
        ("DDL测试", test_basic_ddl),
        ("DML测试", test_basic_dml),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            status = "✅ 通过" if result else "❌ 失败"
            results.append((name, status, result))
            print(f"{name}: {status}")
        except Exception as e:
            results.append((name, f"💥 异常: {e}", False))
            print(f"{name}: 💥 异常: {e}")

    return results


def run_complex_tests():
    """运行复杂查询测试"""
    print("\n🧪 运行复杂查询测试...")
    print("=" * 60)

    tests = [
        ("复杂查询测试", test_complex_queries),
        ("高级功能测试", test_advanced_features),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            status = "✅ 通过" if result else "❌ 失败"
            results.append((name, status, result))
            print(f"{name}: {status}")
        except Exception as e:
            results.append((name, f"💥 异常: {e}", False))
            print(f"{name}: 💥 异常: {e}")

    return results


def run_error_tests():
    """运行错误处理测试"""
    print("\n🧪 运行错误处理测试...")
    print("=" * 60)

    try:
        result = test_error_handling()
        status = "✅ 通过" if result else "❌ 失败"
        print(f"错误处理测试: {status}")
        return [("错误处理测试", status, result)]
    except Exception as e:
        error_msg = f"💥 异常: {e}"
        print(f"错误处理测试: {error_msg}")
        return [("错误处理测试", error_msg, False)]


def run_business_tests():
    """运行业务场景测试"""
    print("\n🧪 运行业务场景测试...")
    print("=" * 60)

    try:
        result = test_business_scenarios()
        status = "✅ 通过" if result else "❌ 失败"
        print(f"业务场景测试: {status}")
        return [("业务场景测试", status, result)]
    except Exception as e:
        error_msg = f"💥 异常: {e}"
        print(f"业务场景测试: {error_msg}")
        return [("业务场景测试", error_msg, False)]


def run_perf_tests():
    """运行性能测试"""
    print("\n🧪 运行性能测试...")
    print("=" * 60)

    try:
        result = run_performance_test()
        status = "✅ 通过" if result else "❌ 失败"
        print(f"性能测试: {status}")
        return [("性能测试", status, result)]
    except Exception as e:
        error_msg = f"💥 异常: {e}"
        print(f"性能测试: {error_msg}")
        return [("性能测试", error_msg, False)]


def run_extended_tests():
    """运行扩展测试（不包括性能测试）"""
    print("🚀 运行扩展测试...")
    print("=" * 60)

    all_results = []

    # 运行各种测试
    all_results.extend(run_basic_tests())
    all_results.extend(run_complex_tests())
    all_results.extend(run_error_tests())
    all_results.extend(run_business_tests())

    # 打印总结
    print_summary(all_results)
    return all_results


def run_complete_tests():
    """运行完整测试（包括性能测试）"""
    print("🚀 运行完整测试套件...")
    print("=" * 60)

    all_results = []

    # 运行各种测试
    all_results.extend(run_basic_tests())
    all_results.extend(run_complex_tests())
    all_results.extend(run_error_tests())
    all_results.extend(run_business_tests())
    all_results.extend(run_perf_tests())

    # 打印总结
    print_summary(all_results)
    return all_results


def print_summary(results):
    """打印测试总结"""
    print("\n" + "🎯" * 30)
    print("测试总结")
    print("🎯" * 30)

    total = len(results)
    passed = sum(1 for _, _, result in results if result)
    failed = total - passed

    print(f"总测试组数: {total}")
    print(f"通过: {passed} ✅")
    print(f"失败: {failed} ❌")

    if total > 0:
        success_rate = (passed / total) * 100
        print(f"成功率: {success_rate:.1f}%")

    if failed > 0:
        print("\n❌ 失败的测试组:")
        for name, status, _ in results:
            if not any(s in status for s in ["✅", "通过"]):
                print(f"  • {name}: {status}")


def show_help():
    """显示帮助信息"""
    print("""
SQL编译器测试运行器

使用方法:
  python run.py [选项]

选项:
  --basic-test     运行基础测试 (DDL, DML)
  --complex-test   运行复杂查询测试
  --error-test     运行错误处理测试
  --business-test  运行业务场景测试
  --perf-test      运行性能测试
  --extended-test  运行扩展测试 (基础+复杂+错误+业务)
  --all-test       运行完整测试 (包括性能测试)
  --help           显示帮助信息

如果没有指定选项，默认运行完整测试套件
    """)


if __name__ == "__main__":
    # 解析命令行参数
    if "--help" in sys.argv or "-h" in sys.argv:
        show_help()
    elif "--basic-test" in sys.argv:
        run_basic_tests()
    elif "--complex-test" in sys.argv:
        run_complex_tests()
    elif "--error-test" in sys.argv:
        run_error_tests()
    elif "--business-test" in sys.argv:
        run_business_tests()
    elif "--perf-test" in sys.argv:
        run_perf_tests()
    elif "--extended-test" in sys.argv:
        run_extended_tests()
    elif "--all-test" in sys.argv:
        run_complete_tests()
    else:
        # 默认运行完整测试
        run_complete_tests()