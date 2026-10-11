"""
AgentSquad Server Supervisor
============================
Keeps the FastAPI uvicorn server running permanently with auto-restart on Windows.
"""
import subprocess
import sys
import time

python_exe = sys.executable

def main():
    print(f"AgentSquad Supervisor started using: {python_exe}")
    while True:
        try:
            print("\n[Supervisor] Launching AgentSquad server (main.py)...")
            proc = subprocess.run([python_exe, "main.py"])
            print(f"[Supervisor] Server process exited with code {proc.returncode}. Restarting in 1s...")
            time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Supervisor] Received interrupt, stopping server.")
            break
        except Exception as e:
            print(f"[Supervisor] Unexpected exception: {e}. Restarting in 2s...")
            time.sleep(2)

if __name__ == "__main__":
    main()
