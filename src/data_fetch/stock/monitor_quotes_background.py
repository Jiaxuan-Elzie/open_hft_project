import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
PYTHON_BIN = Path(sys.executable).resolve()
TARGET_SCRIPT = Path(__file__).with_name("test_minimum_option_quotes.py")
RUNTIME_DIR = WORKSPACE_ROOT / "logs" / "data_fetch" / "stock_quotes"
PID_FILE = RUNTIME_DIR / "quotes.pid"
LOG_FILE = RUNTIME_DIR / "quotes.log"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Start and monitor test_minimum_option_quotes.py as a background process."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser("start", help="Start the quotes downloader in the background.")
    start_parser.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to test_minimum_option_quotes.py. Prefix with --, for example: start -- --mode download",
    )

    subparsers.add_parser("status", help="Show whether the background downloader is still running.")

    tail_parser = subparsers.add_parser("tail", help="Print the latest log output or follow it live.")
    tail_parser.add_argument("--lines", type=int, default=40, help="How many trailing log lines to show first.")
    tail_parser.add_argument("--follow", action="store_true", help="Keep streaming appended log output.")

    subparsers.add_parser("stop", help="Stop the background downloader if it is running.")

    return parser


def ensure_runtime_dir() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def read_pid() -> int | None:
    if not PID_FILE.exists():
        return None
    content = PID_FILE.read_text(encoding="utf-8").strip()
    if not content:
        return None
    try:
        return int(content)
    except ValueError:
        return None


def pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def write_pid(pid: int) -> None:
    PID_FILE.write_text(f"{pid}\n", encoding="utf-8")


def remove_pid_file() -> None:
    if PID_FILE.exists():
        PID_FILE.unlink()


def normalize_script_args(script_args: list[str]) -> list[str]:
    if script_args and script_args[0] == "--":
        return script_args[1:]
    return script_args


def start_background(script_args: list[str]) -> int:
    ensure_runtime_dir()

    existing_pid = read_pid()
    if existing_pid is not None and pid_is_running(existing_pid):
        raise RuntimeError(f"quotes downloader is already running with pid {existing_pid}")

    normalized_args = normalize_script_args(script_args)
    command = [str(PYTHON_BIN), str(TARGET_SCRIPT), *normalized_args]

    log_handle = open(LOG_FILE, "a", encoding="utf-8")
    log_handle.write(
        f"\n===== START {time.strftime('%Y-%m-%d %H:%M:%S')} | {' '.join(command)} =====\n"
    )
    log_handle.flush()

    process = subprocess.Popen(
        command,
        cwd=str(WORKSPACE_ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        text=True,
    )
    write_pid(process.pid)
    log_handle.close()
    return process.pid


def status_background() -> tuple[bool, int | None]:
    pid = read_pid()
    if pid is None:
        return False, None
    if not pid_is_running(pid):
        remove_pid_file()
        return False, pid
    return True, pid


def tail_file(lines: int, follow: bool) -> None:
    ensure_runtime_dir()
    if not LOG_FILE.exists():
        raise FileNotFoundError(f"log file does not exist: {LOG_FILE}")

    with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as handle:
        all_lines = handle.readlines()
        for line in all_lines[-max(0, lines):]:
            sys.stdout.write(line)
        sys.stdout.flush()

        if not follow:
            return

        while True:
            line = handle.readline()
            if line:
                sys.stdout.write(line)
                sys.stdout.flush()
            else:
                running, _ = status_background()
                if not running:
                    remainder = handle.read()
                    if remainder:
                        sys.stdout.write(remainder)
                        sys.stdout.flush()
                    break
                time.sleep(1.0)


def stop_background() -> int | None:
    pid = read_pid()
    if pid is None:
        return None
    if not pid_is_running(pid):
        remove_pid_file()
        return pid

    os.killpg(pid, signal.SIGTERM)

    deadline = time.time() + 10.0
    while time.time() < deadline:
        if not pid_is_running(pid):
            remove_pid_file()
            return pid
        time.sleep(0.5)

    os.killpg(pid, signal.SIGKILL)
    remove_pid_file()
    return pid


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "start":
        pid = start_background(args.script_args)
        print(f"started quotes downloader in background, pid={pid}")
        print(f"log: {LOG_FILE}")
        return

    if args.command == "status":
        running, pid = status_background()
        if running:
            print(f"quotes downloader is running, pid={pid}")
        elif pid is None:
            print("quotes downloader is not running")
        else:
            print(f"quotes downloader is not running; last pid was {pid}")
        print(f"log: {LOG_FILE}")
        return

    if args.command == "tail":
        tail_file(lines=args.lines, follow=args.follow)
        return

    if args.command == "stop":
        pid = stop_background()
        if pid is None:
            print("quotes downloader is not running")
        else:
            print(f"stopped quotes downloader, pid={pid}")
        return

    raise RuntimeError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()