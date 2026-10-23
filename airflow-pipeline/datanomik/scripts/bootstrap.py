"""Create ONLY new isolated Airflow infrastructure credentials, without printing them."""

import base64
import os
import secrets
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1] / ".runtime"
    root.mkdir(mode=0o700, exist_ok=True)
    path = root / "stack.env"
    if path.exists():
        print("Runtime configuration already exists; preserved.")
        return
    values = {
        "AIRFLOW_UID": str(os.getuid()),
        "DATANOMIK_POSTGRES_PASSWORD": secrets.token_hex(32),
        "DATANOMIK_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        "DATANOMIK_JWT_SECRET": secrets.token_hex(32),
        "DATANOMIK_API_SECRET": secrets.token_hex(32),
    }
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write("".join(f"{key}={value}\n" for key, value in values.items()))
    print("Isolated Airflow runtime configuration created. Datanomik credentials were not accessed.")


if __name__ == "__main__":
    main()
