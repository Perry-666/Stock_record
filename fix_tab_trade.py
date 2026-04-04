with open("app.py", "r") as f:
    lines = f.readlines()

for i in range(941, 988):
    if lines[i].strip():
        lines[i] = "    " + lines[i]

with open("app.py", "w") as f:
    f.writelines(lines)
