import glob
import sys
import subprocess

modules_key = [value.split("/")[-1] for value in glob.glob("pyspark_cr7/*")]
modules_path = glob.glob("pyspark_cr7/**/main.py")

modules = dict(zip(modules_key, modules_path))

print(modules)

arg = sys.argv[1]

if arg in modules:
    subprocess.Popen(["uv", "run", modules[arg]])
else:
    print(f"Module {arg} not found!")
    print(f"Available modules: {(modules.keys())}")
