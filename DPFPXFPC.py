# Check memory before and during
with open("/proc/meminfo") as f:
    for line in f:
        if line.startswith(("MemTotal", "MemAvailable", "SwapTotal", "SwapFree")):
            print(line.rstrip(), flush=True)
