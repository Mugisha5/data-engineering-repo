# ...existing code...
from pathlib import Path

script_dir = Path(__file__).parent
curr_file = Path(__file__).name

def safe_read_text(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "utf-16"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")

for file_path in script_dir.iterdir():
    if not file_path.is_file():
        continue
    if file_path.name == curr_file:
        continue
    print(file_path.name)
    content = safe_read_text(file_path)
    print(content)
# ...existing code...