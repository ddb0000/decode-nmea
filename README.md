# AIS NMEA Decoder

Decodes AIS NMEA sentences and stores the results.

## Setup

1. Venv:
```bash
python -m venv venv
source venv/bin/activate
```

2. Install:
```bash
pip install pyais pandas pyarrow
```

3. Run:
```bash
python main-parquet.py
```

## Nix

```bash
nix-shell
```

```bash
python main-parquet.py
```