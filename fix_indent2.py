with open("app.py", "r") as f:
    lines = f.readlines()

for i in range(726, 789):
    if lines[i].startswith("        "): # 8 spaces
        lines[i] = lines[i][4:]

with open("app.py", "w") as f:
    f.writelines(lines)
