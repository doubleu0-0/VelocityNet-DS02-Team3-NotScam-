import subprocess

def test_linting_with_flake8():
    """
    Check code linting using flake8.
    Ensures that Python code adheres to PEP 8 standards.
    """
    result = subprocess.run(["flake8", "--max-line-length=88"], capture_output=True, text=True)
    assert result.returncode == 0, f"Flake8 failed:\n{result.stdout}"

def test_format_with_black():
    """
    Check if the code is formatted correctly using black.
    Ensures all Python code conforms to black formatting.
    """
    result = subprocess.run(["black", "--check", "."], capture_output=True, text=True)
    assert result.returncode == 0, f"Black formatting check failed:\n{result.stdout}"

def test_static_analysis_with_pylint():
    """
    Run static analysis using pylint.
    Checks the code for static analysis issues such as bad practices.
    """
    result = subprocess.run(["pylint", "--disable=C,R", "."], capture_output=True, text=True)
    assert result.returncode == 0, f"Pylint check failed:\n{result.stdout}"
