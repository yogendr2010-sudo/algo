"""
scripts/check_sdk.py
Run this to see exactly what your installed Upstox SDK supports.
Usage: python scripts/check_sdk.py
"""
import sys
import inspect

try:
    import upstox_client
    print(f"✅ upstox_client imported")
    print(f"   Version: {getattr(upstox_client, '__version__', 'unknown')}")
    print()
except ImportError as e:
    print(f"❌ Cannot import upstox_client: {e}")
    sys.exit(1)

# Check MarketDataStreamer
print("=== MarketDataStreamer ===")
if hasattr(upstox_client, "MarketDataStreamer"):
    cls = upstox_client.MarketDataStreamer
    print(f"✅ Found: upstox_client.MarketDataStreamer")
    try:
        sig = inspect.signature(cls.__init__)
        print(f"   __init__ signature: {sig}")
    except Exception:
        pass
    methods = [m for m in dir(cls) if not m.startswith("__")]
    print(f"   Methods/attrs: {methods}")

    # Check if it uses .on() or property callbacks
    inst_methods = [m for m in dir(cls) if "on_" in m or m == "on"]
    print(f"   Event methods: {inst_methods}")
else:
    print("❌ MarketDataStreamer NOT found")

# Check for V3 streamer
if hasattr(upstox_client, "MarketDataStreamerV3"):
    print("✅ Also found: MarketDataStreamerV3")

print()
print("=== Order APIs ===")
for name in ["OrderApi", "OrderApiV3"]:
    if hasattr(upstox_client, name):
        print(f"✅ {name}")

print()
print("=== History APIs ===")
for name in ["HistoryApi", "HistoryV3Api"]:
    if hasattr(upstox_client, name):
        print(f"✅ {name}")
        cls = getattr(upstox_client, name)
        methods = [m for m in dir(cls) if "candle" in m.lower() or "intra" in m.lower()]
        if methods:
            for m in methods:
                try:
                    sig = inspect.signature(getattr(cls, m))
                    print(f"   {m}{sig}")
                except Exception:
                    print(f"   {m}")

print()
print("=== Order Request models ===")
for name in ["PlaceOrderRequest", "PlaceOrderV3Request"]:
    if hasattr(upstox_client, name):
        cls = getattr(upstox_client, name)
        print(f"✅ {name}")
        try:
            sig = inspect.signature(cls.__init__)
            print(f"   Fields: {list(sig.parameters.keys())}")
        except Exception:
            pass

print()
print("=== Streamer source file ===")
if hasattr(upstox_client, "MarketDataStreamer"):
    try:
        f = inspect.getfile(upstox_client.MarketDataStreamer)
        print(f"   {f}")
        with open(f) as fp:
            print("\n--- First 80 lines of market_data_streamer.py ---")
            for i, line in enumerate(fp):
                if i >= 80:
                    break
                print(f"  {i+1:3}: {line}", end="")
    except Exception as e:
        print(f"   Could not read source: {e}")
