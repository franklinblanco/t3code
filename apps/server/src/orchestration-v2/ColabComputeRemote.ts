// Only this supervisor uses the VM's SSH root account. Each job runs as a disposable Linux user.
export const supervisor = String.raw`
import fcntl
import json
import os
import pathlib
import pwd
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time

action, job_id, *args = sys.argv[1:]
request = json.load(sys.stdin) if action == "run" else None
system_path = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
os.environ["PATH"] = system_path
base = pathlib.Path("/content/t3-jobs")
control = base / ".control"
root = base / job_id
user = "t3-" + job_id.replace("-", "")[:24]
base.mkdir(mode=0o711, exist_ok=True)
control.mkdir(mode=0o700, exist_ok=True)
state = control / job_id

def change_account(command):
    with (control / ".passwd.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        subprocess.run(command, check=True)

def checked_path(relative, allow_symlinks=False):
    path = root / relative
    resolved = path.resolve(strict=True)
    if (not allow_symlinks and resolved != path) or not resolved.is_relative_to(root):
        raise ValueError("Job paths cannot traverse symlinks or leave the job directory")
    return resolved

def kill_job(account, pgid):
    if pgid:
        try:
            if os.stat("/proc/" + str(pgid)).st_uid == account.pw_uid:
                os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, FileNotFoundError):
            pass
    uid = str(account.pw_uid)
    for _ in range(20):
        killed = subprocess.run(["pkill", "-KILL", "-u", uid], check=False)
        if killed.returncode not in (0, 1):
            raise RuntimeError("Could not signal the Colab job's processes")
        remaining = subprocess.run(["pgrep", "-u", uid], stdout=subprocess.DEVNULL, check=False)
        if remaining.returncode == 1:
            return
        remaining.check_returncode()
        time.sleep(0.1)
    raise RuntimeError("Colab job processes survived cleanup")

def start_job(command, cwd, env, account, stdin, pass_fds=()):
    return subprocess.Popen(
        command, cwd=cwd, env=env, stdin=stdin, user=account.pw_uid,
        group=account.pw_gid, extra_groups=[], start_new_session=True, pass_fds=pass_fds,
    )

def collect_artifact(archive, path):
    entry = archive.gettarinfo(str(path), str(path.relative_to(root)))
    if entry.isdir():
        archive.addfile(entry)
        for child in sorted(path.iterdir()):
            collect_artifact(archive, child)
    elif entry.isreg() and path.lstat().st_nlink == 1:
        with path.open("rb") as source:
            archive.addfile(entry, source)
    else:
        raise ValueError("Artifacts may contain only regular files and directories")

child = None
with state.open("a+") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    lock.seek(0)
    status = lock.read().strip()
    if status == "closed" and action != "clean":
        sys.exit("Job directory is closed")

    if action == "prepare":
        change_account(["useradd", "-M", "-d", str(root), user])
        account = pwd.getpwnam(user)
        root.mkdir(mode=0o700)
        os.chown(root, account.pw_uid, account.pw_gid)
    elif action == "clean":
        if status != "closed":
            try:
                account = pwd.getpwnam(user)
            except KeyError:
                account = None
            if account:
                kill_job(account, int(status) if status.isdigit() else None)
                change_account(["userdel", user])
            if root.exists():
                shutil.rmtree(root)
            lock.seek(0)
            lock.truncate()
            lock.write("closed")
    elif action == "artifacts":
        account = pwd.getpwnam(user)
        kill_job(account, None)
        os.setgroups([])
        os.setgid(account.pw_gid)
        os.setuid(account.pw_uid)
        with tarfile.open(fileobj=sys.stdout.buffer, mode="w|gz") as archive:
            for relative in json.loads(args[0]):
                collect_artifact(archive, checked_path(relative))
    else:
        account = pwd.getpwnam(user)
        env = {
            "HOME": str(root),
            "USER": user,
            "LOGNAME": user,
            "LANG": "C.UTF-8",
            "PATH": system_path,
        }
        if action == "upload":
            command = [sys.executable, "-c",
                "import sys,tarfile; tarfile.open(fileobj=sys.stdin.buffer, mode='r|gz').extractall(filter='data')"]
            child = start_job(command, root, env, account, sys.stdin.buffer)
        elif action == "run":
            cwd = checked_path(request["cwd"], allow_symlinks=True)
            env.update(request["env"])
            # The anonymous script is readable only by this job's account and never enters argv.
            with tempfile.TemporaryFile(dir=control) as script:
                script.write(request["command"].encode("utf-8"))
                script.flush()
                script.seek(0)
                fd = script.fileno()
                os.fchown(fd, account.pw_uid, account.pw_gid)
                child = start_job(
                    ["/bin/bash", "-lc", 'eval -- "$(< /proc/self/fd/' + str(fd) + ')"'],
                    cwd, env, account,
                    subprocess.DEVNULL, pass_fds=(fd,),
                )
        else:
            sys.exit("Unknown job operation")
        lock.seek(0)
        lock.truncate()
        lock.write(str(child.pid))

if child:
    # Keep a closed job closed if cleanup won the race while the command exited.
    with state.open("a+") as lock:
        result = child.wait()
        fcntl.flock(lock, fcntl.LOCK_EX)
        lock.seek(0)
        if lock.read().strip() == str(child.pid):
            lock.seek(0)
            lock.truncate()
        sys.exit(result if result >= 0 else 128 - result)
`;

export const probe = String.raw`
import json, os
with open("/proc/loadavg") as file:
    cpu = float(file.read().split()[0]) / len(os.sched_getaffinity(0))
with open("/proc/meminfo") as file:
    memory = {line.split(":")[0]: int(line.split()[1]) for line in file}
print(json.dumps({"cpu": cpu, "memoryAvailable": memory["MemAvailable"] / memory["MemTotal"]}))
`;
