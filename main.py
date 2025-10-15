import subprocess
import sys

arg = sys.argv[1]

modules = {"scd2": "pyspark_cr7/scd2/main.py", "scd1": "pyspark_cr7/scd1/main.py"}

if arg in modules:
    subprocess.Popen(["uv", "run", modules[arg]])
else:
    print(f"Module {arg} not found!")
    print(f"Available modules: {(modules.keys())}")
