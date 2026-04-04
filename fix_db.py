import sqlite3
import re

def normalize_stock_id(s):
    s = str(s).strip().upper()
    if s.endswith(".TW"):
        s = s[:-3]
    if s.endswith(".TWO"):
        s = s[:-4]
    s = re.split(r'[\s\-]', s)[0].strip()
    return s

conn = sqlite3.connect("trading_system.db")
c = conn.cursor()
c.execute("SELECT id, stock_id FROM Trades")
rows = c.fetchall()
for r in rows:
    new_id = normalize_stock_id(r[1])
    if new_id != r[1]:
        c.execute("UPDATE Trades SET stock_id = ? WHERE id = ?", (new_id, r[0]))
conn.commit()
conn.close()
print("DB fixed!")
