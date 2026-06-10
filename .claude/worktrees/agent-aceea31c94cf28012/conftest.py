import sys

# os.stat('') raises FileNotFoundError on Windows; strip empty entries before
# importlib.metadata scans sys.path for installed packages.
sys.path[:] = [p for p in sys.path if p]
