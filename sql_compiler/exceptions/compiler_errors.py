class CompilerError(Exception):
    """编译器基础异常类"""
    def __init__(self, message: str, line: int = 0, column: int = 0):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(f"[行 {line}, 列 {column}] {message}")

class LexicalError(CompilerError):
    """词法分析错误"""
    pass

class SyntaxError(CompilerError):
    """语法分析错误"""
    def __init__(self, message: str, line: int = 0, column: int = 0, expected: str = None):
        self.expected = expected
        if expected:
            message = f"{message}，期望: {expected}"
        super().__init__(message, line, column)

class SemanticError(CompilerError):
    """语义分析错误"""
    pass