import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
TARGET_SCRIPT = Path(__file__).with_name("raw_data_fetch.py")
PYTHON_BIN = Path(sys.executable).resolve()

RUNTIME_DIR = PROJECT_ROOT / "logs" / "data_fetch" / "fut_guard"
GUARD_PID_FILE = RUNTIME_DIR / "guard.pid"
CHILD_PID_FILE = RUNTIME_DIR / "child.pid"
GUARD_LOG_FILE = RUNTIME_DIR / "guard.log"
CHILD_LOG_FILE = RUNTIME_DIR / "child.log"


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Supervise raw_data_fetch.py and restart it automatically when it exits with a non-zero code.")
	subparsers = parser.add_subparsers(dest="command", required=True)

	start_parser = subparsers.add_parser("start", help="Start the guard in the background.")
	start_parser.add_argument("--restart-delay", type=int, default=10, help="Seconds to wait before restarting after a failed run.")
	start_parser.add_argument("--max-restarts", type=int, default=0, help="Maximum restart attempts. Use 0 for unlimited restarts.")
	start_parser.add_argument(
		"script_args",
		nargs=argparse.REMAINDER,
		help="Arguments passed through to raw_data_fetch.py. Prefix with --, for example: start -- --mode both",
	)

	run_parser = subparsers.add_parser("run", help="Run the guard in the current terminal.")
	run_parser.add_argument("--restart-delay", type=int, default=10, help="Seconds to wait before restarting after a failed run.")
	run_parser.add_argument("--max-restarts", type=int, default=0, help="Maximum restart attempts. Use 0 for unlimited restarts.")
	run_parser.add_argument(
		"script_args",
		nargs=argparse.REMAINDER,
		help="Arguments passed through to raw_data_fetch.py. Prefix with --, for example: run -- --mode both",
	)

	subparsers.add_parser("status", help="Show whether the guard is running.")

	tail_parser = subparsers.add_parser("tail", help="Print the latest guard log output or follow it live.")
	tail_parser.add_argument("--lines", type=int, default=40, help="How many trailing log lines to show first.")
	tail_parser.add_argument("--follow", action="store_true", help="Keep streaming appended guard log output.")

	subparsers.add_parser("stop", help="Stop the guard and any child downloader it manages.")
	return parser


def ensure_runtime_dir() -> None:
	RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def normalize_script_args(script_args: list[str]) -> list[str]:
	if script_args and script_args[0] == "--":
		return script_args[1:]
	return script_args


def write_log(log_path: Path, message: str) -> None:
	ensure_runtime_dir()
	with open(log_path, "a", encoding="utf-8") as handle:
		handle.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}\n")


def read_pid(pid_path: Path) -> int | None:
	if not pid_path.exists():
		return None
	content = pid_path.read_text(encoding="utf-8").strip()
	if not content:
		return None
	try:
		return int(content)
	except ValueError:
		return None


def write_pid(pid_path: Path, pid: int) -> None:
	ensure_runtime_dir()
	pid_path.write_text(f"{pid}\n", encoding="utf-8")


def remove_pid(pid_path: Path) -> None:
	if pid_path.exists():
		pid_path.unlink()


def pid_is_running(pid: int) -> bool:
	try:
		os.kill(pid, 0)
		return True
	except OSError:
		return False


def child_command(script_args: list[str]) -> list[str]:
	return [str(PYTHON_BIN), str(TARGET_SCRIPT), *normalize_script_args(script_args)]


def start_child(script_args: list[str]) -> subprocess.Popen:
	ensure_runtime_dir()
	command = child_command(script_args)
	log_handle = open(CHILD_LOG_FILE, "a", encoding="utf-8")
	log_handle.write(f"\n===== START {time.strftime('%Y-%m-%d %H:%M:%S')} | {' '.join(command)} =====\n")
	log_handle.flush()
	process = subprocess.Popen(
		command,
		cwd=str(PROJECT_ROOT),
		stdout=log_handle,
		stderr=subprocess.STDOUT,
		stdin=subprocess.DEVNULL,
		start_new_session=True,
		text=True,
	)
	write_pid(CHILD_PID_FILE, process.pid)
	log_handle.close()
	return process


def read_child_log_slice(start_offset: int) -> str:
	if not CHILD_LOG_FILE.exists():
		return ""
	with open(CHILD_LOG_FILE, "r", encoding="utf-8", errors="replace") as handle:
		handle.seek(start_offset)
		return handle.read()


def run_guard(script_args: list[str], restart_delay: int, max_restarts: int) -> None:
	ensure_runtime_dir()
	existing_guard_pid = read_pid(GUARD_PID_FILE)
	if existing_guard_pid is not None and existing_guard_pid != os.getpid() and pid_is_running(existing_guard_pid):
		raise RuntimeError(f"raw_data_fetch guard is already running with pid {existing_guard_pid}")

	write_pid(GUARD_PID_FILE, os.getpid())
	write_log(GUARD_LOG_FILE, "guard started")
	restart_count = 0
	normalized_args = normalize_script_args(script_args)

	try:
		while True:
			start_offset = CHILD_LOG_FILE.stat().st_size if CHILD_LOG_FILE.exists() else 0
			process = start_child(normalized_args)
			write_log(GUARD_LOG_FILE, f"started child pid={process.pid}")
			exit_code = process.wait()

			current_child_pid = read_pid(CHILD_PID_FILE)
			if current_child_pid == process.pid:
				remove_pid(CHILD_PID_FILE)

			_ = read_child_log_slice(start_offset)
			if exit_code == 0:
				write_log(GUARD_LOG_FILE, f"child pid={process.pid} exited cleanly with code 0; guard stopping")
				return

			restart_count += 1
			write_log(
				GUARD_LOG_FILE,
				f"child pid={process.pid} exited with code {exit_code}; restart {restart_count} scheduled in {restart_delay}s",
			)

			if max_restarts > 0 and restart_count >= max_restarts:
				write_log(GUARD_LOG_FILE, "maximum restart count reached; guard stopping")
				return

			time.sleep(max(0, restart_delay))
	finally:
		remove_pid(GUARD_PID_FILE)
		remove_pid(CHILD_PID_FILE)
		write_log(GUARD_LOG_FILE, "guard stopped")


def start_guard_background(script_args: list[str], restart_delay: int, max_restarts: int) -> int:
	ensure_runtime_dir()
	existing_guard_pid = read_pid(GUARD_PID_FILE)
	if existing_guard_pid is not None and pid_is_running(existing_guard_pid):
		raise RuntimeError(f"raw_data_fetch guard is already running with pid {existing_guard_pid}")

	command = [
		str(PYTHON_BIN),
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
	log_handle.write(f"\n===== GUARD START {time.strftime('%Y-%m-%d %H:%M:%S')} | {' '.join(command)} =====\n")
	log_handle.flush()
	process = subprocess.Popen(
		command,
		cwd=str(PROJECT_ROOT),
		stdout=log_handle,
		stderr=subprocess.STDOUT,
		stdin=subprocess.DEVNULL,
		start_new_session=True,
		text=True,
	)
	log_handle.close()
	write_pid(GUARD_PID_FILE, process.pid)
	return process.pid


def guard_status() -> tuple[bool, int | None]:
	pid = read_pid(GUARD_PID_FILE)
	if pid is None:
		return False, None
	if not pid_is_running(pid):
		remove_pid(GUARD_PID_FILE)
		return False, pid
	return True, pid


def tail_guard_log(lines: int, follow: bool) -> None:
	ensure_runtime_dir()
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
	child_pid = read_pid(CHILD_PID_FILE)
	if child_pid is not None and pid_is_running(child_pid):
		os.killpg(child_pid, signal.SIGTERM)

	guard_pid = read_pid(GUARD_PID_FILE)
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

	remove_pid(GUARD_PID_FILE)
	remove_pid(CHILD_PID_FILE)
	return guard_pid, child_pid


def main() -> None:
	args = build_parser().parse_args()

	if args.command == "start":
		pid = start_guard_background(args.script_args, args.restart_delay, args.max_restarts)
		print(f"started raw_data_fetch guard in background, pid={pid}")
		print(f"guard log: {GUARD_LOG_FILE}")
		print(f"child log: {CHILD_LOG_FILE}")
		return

	if args.command == "run":
		run_guard(args.script_args, args.restart_delay, args.max_restarts)
		return

	if args.command == "status":
		running, pid = guard_status()
		child_pid = read_pid(CHILD_PID_FILE)
		if running:
			print(f"raw_data_fetch guard is running, pid={pid}")
		elif pid is None:
			print("raw_data_fetch guard is not running")
		else:
			print(f"raw_data_fetch guard is not running; last pid was {pid}")
		if child_pid is not None and pid_is_running(child_pid):
			print(f"managed downloader pid={child_pid}")
		print(f"guard log: {GUARD_LOG_FILE}")
		print(f"child log: {CHILD_LOG_FILE}")
		return

	if args.command == "tail":
		tail_guard_log(lines=args.lines, follow=args.follow)
		return

	if args.command == "stop":
		guard_pid, child_pid = stop_guard()
		if guard_pid is None and child_pid is None:
			print("raw_data_fetch guard is not running")
		else:
			print(f"stopped raw_data_fetch guard pid={guard_pid}, child pid={child_pid}")
		return

	raise RuntimeError(f"unsupported command: {args.command}")


if __name__ == "__main__":
	main()