import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import monitor_quotes_background as monitor


GUARD_RUNTIME_DIR = monitor.RUNTIME_DIR / "guard"
GUARD_PID_FILE = GUARD_RUNTIME_DIR / "guard.pid"
GUARD_LOG_FILE = GUARD_RUNTIME_DIR / "guard.log"
CHILD_PID_FILE = monitor.PID_FILE
CHILD_LOG_FILE = monitor.LOG_FILE
RESTART_PATTERNS = (
    "ResponseStreamingError",
    "IncompleteRead",
    "ProtocolError",
    "Connection broken:",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Supervise test_minimum_option_quotes.py and auto-restart it when streaming HTTP download errors occur."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser("start", help="Start the quotes guard in the background.")
    start_parser.add_argument("--restart-delay", type=int, default=10, help="Seconds to wait before restarting after a matching HTTP streaming error.")
    start_parser.add_argument("--max-restarts", type=int, default=0, help="Maximum automatic restarts. Use 0 for unlimited restarts.")
    start_parser.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to test_minimum_option_quotes.py. Prefix with --, for example: start -- --mode download",
    )

    run_parser = subparsers.add_parser("run", help="Run the quotes guard in the current terminal.")
    run_parser.add_argument("--restart-delay", type=int, default=10, help="Seconds to wait before restarting after a matching HTTP streaming error.")
    run_parser.add_argument("--max-restarts", type=int, default=0, help="Maximum automatic restarts. Use 0 for unlimited restarts.")
    run_parser.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to test_minimum_option_quotes.py. Prefix with --, for example: run -- --mode download",
    )

    subparsers.add_parser("status", help="Show whether the quotes guard is running.")

    tail_parser = subparsers.add_parser("tail", help="Print the latest guard log output or follow it live.")
    tail_parser.add_argument("--lines", type=int, default=40, help="How many trailing log lines to show first.")
    tail_parser.add_argument("--follow", action="store_true", help="Keep streaming appended guard log output.")

    subparsers.add_parser("stop", help="Stop the quotes guard and any child downloader it manages.")

    return parser


def ensure_guard_runtime_dir() -> None:
    GUARD_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def read_guard_pid() -> int | None:
    if not GUARD_PID_FILE.exists():
        return None
    content = GUARD_PID_FILE.read_text(encoding="utf-8").strip()
    if not content:
        return None
    try:
        return int(content)
    except ValueError:
        return None


def write_guard_pid(pid: int) -> None:
    ensure_guard_runtime_dir()
    GUARD_PID_FILE.write_text(f"{pid}\n", encoding="utf-8")


def remove_guard_pid() -> None:
    if GUARD_PID_FILE.exists():
        GUARD_PID_FILE.unlink()


def write_guard_log(message: str) -> None:
    ensure_guard_runtime_dir()
    with open(GUARD_LOG_FILE, "a", encoding="utf-8") as handle:
        handle.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}\n")


def pid_is_running(pid: int) -> bool:
    return monitor.pid_is_running(pid)


def normalize_script_args(script_args: list[str]) -> list[str]:
    return monitor.normalize_script_args(script_args)


def child_command(script_args: list[str]) -> list[str]:
    return [str(monitor.PYTHON_BIN), str(monitor.TARGET_SCRIPT), *normalize_script_args(script_args)]


def start_child(script_args: list[str]) -> subprocess.Popen:
    monitor.ensure_runtime_dir()
    command = child_command(script_args)
    log_handle = open(CHILD_LOG_FILE, "a", encoding="utf-8")
    log_handle.write(
        f"\n===== START {time.strftime('%Y-%m-%d %H:%M:%S')} | {' '.join(command)} =====\n"
    )
    log_handle.flush()
    process = subprocess.Popen(
        command,
        cwd=str(monitor.WORKSPACE_ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        text=True,
    )
    monitor.write_pid(process.pid)
    log_handle.close()
    return process


def read_log_slice(start_offset: int) -> str:
    if not CHILD_LOG_FILE.exists():
        return ""
    with open(CHILD_LOG_FILE, "r", encoding="utf-8", errors="replace") as handle:
        handle.seek(start_offset)
        return handle.read()


def matches_restart_error(log_text: str) -> bool:
    return any(pattern in log_text for pattern in RESTART_PATTERNS)


def run_guard(script_args: list[str], restart_delay: int, max_restarts: int) -> None:
    ensure_guard_runtime_dir()
    monitor.ensure_runtime_dir()

    existing_guard_pid = read_guard_pid()
    if existing_guard_pid is not None and existing_guard_pid != os.getpid() and pid_is_running(existing_guard_pid):
        raise RuntimeError(f"quotes guard is already running with pid {existing_guard_pid}")

    write_guard_pid(os.getpid())
    write_guard_log("guard started")

    restart_count = 0
    normalized_args = normalize_script_args(script_args)

    try:
        while True:
            if CHILD_LOG_FILE.exists():
                start_offset = CHILD_LOG_FILE.stat().st_size
            else:
                start_offset = 0

            process = start_child(normalized_args)
            write_guard_log(f"started child pid={process.pid}")
            exit_code = process.wait()

            current_child_pid = monitor.read_pid()
            if current_child_pid == process.pid:
                monitor.remove_pid_file()

            log_text = read_log_slice(start_offset)
            if exit_code == 0:
                write_guard_log(f"child pid={process.pid} exited cleanly with code 0; guard stopping")
                return

            if not matches_restart_error(log_text):
                write_guard_log(
                    f"child pid={process.pid} exited with code {exit_code} without matching HTTP streaming error; guard stopping"
                )
                return

            restart_count += 1
            write_guard_log(
                f"child pid={process.pid} exited with code {exit_code} after matching HTTP streaming error; restart {restart_count} scheduled in {restart_delay}s"
            )

            if max_restarts > 0 and restart_count >= max_restarts:
                write_guard_log("maximum restart count reached; guard stopping")
                return

            time.sleep(max(0, restart_delay))
    finally:
        remove_guard_pid()
        write_guard_log("guard stopped")


def start_guard_background(script_args: list[str], restart_delay: int, max_restarts: int) -> int:
    ensure_guard_runtime_dir()

    existing_guard_pid = read_guard_pid()
    if existing_guard_pid is not None and pid_is_running(existing_guard_pid):
        raise RuntimeError(f"quotes guard is already running with pid {existing_guard_pid}")

    command = [
        str(monitor.PYTHON_BIN),
        str(Path(__file__)),
        "run",
        "--restart-delay",
        str(restart_delay),
        "--max-restarts",
        str(max_restarts),
        "--",
        *normalize_script_args(script_args),
    ]

    log_handle = open(GUARD_LOG_FILE, "a", encoding="utf-8")
    log_handle.write(
        f"\n===== GUARD START {time.strftime('%Y-%m-%d %H:%M:%S')} | {' '.join(command)} =====\n"
    )
    log_handle.flush()

    process = subprocess.Popen(
        command,
        cwd=str(monitor.WORKSPACE_ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        text=True,
    )
    log_handle.close()
    write_guard_pid(process.pid)
    return process.pid


def guard_status() -> tuple[bool, int | None]:
    pid = read_guard_pid()
    if pid is None:
        return False, None
    if not pid_is_running(pid):
        remove_guard_pid()
        return False, pid
    return True, pid


def tail_guard_log(lines: int, follow: bool) -> None:
    ensure_guard_runtime_dir()
    if not GUARD_LOG_FILE.exists():
        raise FileNotFoundError(f"guard log file does not exist: {GUARD_LOG_FILE}")

    with open(GUARD_LOG_FILE, "r", encoding="utf-8", errors="replace") as handle:
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
                running, _ = guard_status()
                if not running:
                    remainder = handle.read()
                    if remainder:
                        sys.stdout.write(remainder)
                        sys.stdout.flush()
                    break
                time.sleep(1.0)


def stop_guard() -> tuple[int | None, int | None]:
    child_pid = monitor.read_pid()
    if child_pid is not None and pid_is_running(child_pid):
        os.killpg(child_pid, signal.SIGTERM)

    guard_pid = read_guard_pid()
    if guard_pid is None:
        return None, child_pid

    if pid_is_running(guard_pid):
        os.killpg(guard_pid, signal.SIGTERM)

    deadline = time.time() + 10.0
    while time.time() < deadline:
        guard_running = guard_pid is not None and pid_is_running(guard_pid)
        child_running = child_pid is not None and pid_is_running(child_pid)
        if not guard_running and not child_running:
            break
        time.sleep(0.5)

    if guard_pid is not None and pid_is_running(guard_pid):
        os.killpg(guard_pid, signal.SIGKILL)
    if child_pid is not None and pid_is_running(child_pid):
        os.killpg(child_pid, signal.SIGKILL)

    remove_guard_pid()
    monitor.remove_pid_file()
    return guard_pid, child_pid


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "start":
        pid = start_guard_background(args.script_args, args.restart_delay, args.max_restarts)
        print(f"started quotes guard in background, pid={pid}")
        print(f"guard log: {GUARD_LOG_FILE}")
        print(f"quotes log: {CHILD_LOG_FILE}")
        return

    if args.command == "run":
        run_guard(args.script_args, args.restart_delay, args.max_restarts)
        return

    if args.command == "status":
        running, pid = guard_status()
        child_pid = monitor.read_pid()
        if running:
            print(f"quotes guard is running, pid={pid}")
        elif pid is None:
            print("quotes guard is not running")
        else:
            print(f"quotes guard is not running; last pid was {pid}")
        if child_pid is not None and pid_is_running(child_pid):
            print(f"managed quotes downloader pid={child_pid}")
        print(f"guard log: {GUARD_LOG_FILE}")
        print(f"quotes log: {CHILD_LOG_FILE}")
        return

    if args.command == "tail":
        tail_guard_log(lines=args.lines, follow=args.follow)
        return

    if args.command == "stop":
        guard_pid, child_pid = stop_guard()
        if guard_pid is None and child_pid is None:
            print("quotes guard is not running")
        else:
            print(f"stopped quotes guard pid={guard_pid}, child pid={child_pid}")
        return

    raise RuntimeError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()