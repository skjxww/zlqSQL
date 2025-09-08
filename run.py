import sys
import os

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 导入并运行SQL编译器
if __name__ == "__main__":
    from sql_compiler.main import run_tests, main,run_multi_line_tests,run_error_tests,run_extended_tests
    if "--test" in sys.argv:
        run_tests()
    elif "--multi-test" in sys.argv:
        run_multi_line_tests()
    elif "--error-test" in sys.argv:
        run_error_tests()
    elif "--extended-test" in sys.argv:
        run_extended_tests()
    elif "--all-test" in sys.argv:
        print("执行所有测试...")
        run_tests()
        print("\n" + "=" * 80 + "\n")
        run_multi_line_tests()
        print("\n" + "=" * 80 + "\n")
        run_error_tests()
    else:
        main()