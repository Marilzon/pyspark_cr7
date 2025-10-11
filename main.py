import subprocess
import sys

args = f"pyspark_cr7/{sys.argv[1]}.py"

subprocess.Popen(["uv", "run", args])