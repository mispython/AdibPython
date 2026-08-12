import shutil

folder = "/path/to/your/folder"

total, used, free = shutil.disk_usage(folder)

print(f"Total: {total / (1024**3):.2f} GB")
print(f"Used:  {used / (1024**3):.2f} GB")
print(f"Free:  {free / (1024**3):.2f} GB")
