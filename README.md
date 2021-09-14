# MP0

---

## Local Development Setup

```bash
python --version # should >= 3.8
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev]"
```

## Usage

```bash
mp0c <name> <host> <port>
mp0c A 127.0.0.1 8080
python ./generator.py 10 50 | mp0c A 127.0.0.1 8080

mp0s <port>
mp0s 8080
```
