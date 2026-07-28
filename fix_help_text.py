# Fix help text - simple line insert
import os
os.chdir('c:/Users/Dev computer/Desktop/algo_option')

with open('option_algo/backend/services/telegram_bot.py', 'rb') as f:
    data = f.read()

# Search for the exact bytes
# Looking for: "/pnl           — Today's P&amp;L summary
marker = b'"/pnl           \xe2\x80\x94 Today'
idx = data.find(marker)
if idx >= 0:
    # Find the end of this line
    line_end = data.find(b'\n', idx)
    # Find the beginning of this line
    line_start = data.rfind(b'\n', 0, idx) + 1
    pnl_line = data[line_start:line_end+1]
    print(f"Found pnl line at byte {idx}")
    
    # Find /help line
    help_start = data.find(b'/help', line_end)
    help_line_end = data.find(b'\n', help_start)
    help_line = data[help_start:help_line_end+1]
    
    # Find <i>Example line
    ex_start = data.find(b'<i>Example', help_line_end)
    ex_end = data.find(b'\n', ex_start)
    ex_line = data[ex_start:ex_end+1]
    
    # Build new content
    indent = b'        '
    new_block = (
        pnl_line +
        indent + b'"/approve_<id>  \xe2\x80\x94 Approve pending trade (e.g. /approve_42)\\n"\n' +
        indent + b'"/reject_<id>   \xe2\x80\x94 Reject pending trade (e.g. /reject_42)\\n"\n' +
        help_line +
        ex_line
    )
    
    old_block = pnl_line + help_line + ex_line
    data = data.replace(old_block, new_block, 1)
    
    with open('option_algo/backend/services/telegram_bot.py', 'wb') as f:
        f.write(data)
    print("SUCCESS: Help text updated!")
else:
    print("FAILED: Could not find marker")
    # Try to find the bytes around /pnl
    idx2 = data.find(b'/pnl')
    if idx2 >= 0:
        print(f"Found /pnl at byte {idx2}")
        print(f"Context: {data[idx2-20:idx2+80]}")
