"""
Start een lokale webserver voor de landkaart.
Draait eerst build_data.py om data.json te genereren,
en opent daarna de browser.

Gebruik: python landkaart/serve.py
"""

import http.server
import os
import subprocess
import sys
import webbrowser
from pathlib import Path

PORT = 8050
DIR = Path(__file__).resolve().parent

# Stap 1: bouw data.json
print("Data voorbereiden...")
subprocess.run([sys.executable, str(DIR / "build_data.py")], check=True)

# Stap 2: start webserver
os.chdir(DIR)
print(f"\nServer draait op http://localhost:{PORT}")
print("Druk Ctrl+C om te stoppen.\n")

webbrowser.open(f"http://localhost:{PORT}")

handler = http.server.SimpleHTTPRequestHandler
with http.server.HTTPServer(("", PORT), handler) as httpd:
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer gestopt.")
