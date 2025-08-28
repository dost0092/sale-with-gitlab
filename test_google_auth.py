#!/usr/bin/env python3
# test_google_auth.py
import os, json, sys

def main():
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    raw = os.environ.get("GOOGLE_CREDENTIALS")
    print("GOOGLE_CREDENTIALS_FILE set?", bool(file_env))
    print("GOOGLE_CREDENTIALS set?", bool(raw))
    if file_env:
        print("GOOGLE_CREDENTIALS_FILE path:", file_env)
        if not os.path.exists(file_env):
            print("ERROR: file path does not exist.")
            sys.exit(2)
        try:
            with open(file_env, "r", encoding="utf-8") as fh:
                info = json.load(fh)
            print("Loaded file JSON. client_email:", info.get("client_email"))
            sys.exit(0)
        except Exception as e:
            print("ERROR reading file JSON:", e)
            sys.exit(2)
    if raw:
        print("GOOGLE_CREDENTIALS length:", len(raw))
        try:
            info = json.loads(raw)
            print("Parsed JSON OK. client_email:", info.get("client_email"))
            sys.exit(0)
        except Exception as e:
            print("ERROR: GOOGLE_CREDENTIALS invalid JSON:", e)
            sys.exit(2)
    print("No usable credentials found.")
    sys.exit(2)

if __name__ == "__main__":
    main()
