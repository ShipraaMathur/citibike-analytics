#!/usr/bin/env python3
"""Stand-ins for `hdfs dfs ...` and `hadoop jar hadoop-streaming.jar ...`.

They map HDFS onto a local folder ($FAKE_HDFS_ROOT) and run Hadoop Streaming
jobs as map -> sort -> reduce pipelines, so the project's shell scripts and
Python jobs can be exercised on a machine without Hadoop. This is a test aid
only. It does not reproduce Hadoop's scheduling, splits or memory limits.
"""
import fnmatch
import glob
import os
import shutil
import subprocess
import sys
import tempfile
import zlib

ROOT = os.environ["FAKE_HDFS_ROOT"]


def local(path):
    return os.path.join(ROOT, path.lstrip("/"))


def expand(pattern):
    hits = sorted(glob.glob(local(pattern)))
    return hits


def hdfs(argv):
    if argv[0] != "dfs":
        sys.exit("unsupported: %s" % argv)
    op, rest = argv[1], argv[2:]
    if op == "-mkdir":
        for p in [a for a in rest if not a.startswith("-")]:
            os.makedirs(local(p), exist_ok=True)
    elif op == "-test":
        sys.exit(0 if os.path.exists(local(rest[-1])) else 1)
    elif op == "-put":
        paths = [a for a in rest if a == "-" or not a.startswith("-")]
        src, dst = paths[0], local(paths[1])
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst + "._COPYING_", "wb") as out:
            if src == "-":
                shutil.copyfileobj(sys.stdin.buffer, out)
            else:
                with open(src, "rb") as fh:
                    shutil.copyfileobj(fh, out)
        os.replace(dst + "._COPYING_", dst)
    elif op == "-cat":
        for pat in rest:
            for p in expand(pat):
                with open(p, "rb") as fh:
                    shutil.copyfileobj(fh, sys.stdout.buffer)
    elif op == "-rm":
        for p in [a for a in rest if not a.startswith("-")]:
            for hit in expand(p):
                shutil.rmtree(hit) if os.path.isdir(hit) else os.remove(hit)
    elif op == "-du":
        human = "-h" in rest
        targets = [a for a in rest if not a.startswith("-")]
        found = False
        for pat in targets:
            for p in expand(pat):
                found = True
                if os.path.isdir(p):
                    size = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(p) for f in fs)
                else:
                    size = os.path.getsize(p)
                shown = ("%.1f M" % (size / 1e6)) if human else str(size)
                print("%s  %s  %s" % (shown, shown, "/" + os.path.relpath(p, ROOT)))
        if not found:
            sys.exit(1)
    elif op == "-df":
        print("Filesystem  Size  Used  Available  Use%")
        print("hdfs://fake  1 T  1 G  1 T  0%")
    else:
        sys.exit("unsupported hdfs op: %s" % op)


def run_streaming(argv):
    opts = {"D": {}, "files": [], "combiner": None, "env": {}}
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "-D":
            k, v = argv[i + 1].split("=", 1)
            opts["D"][k] = v
            i += 2
        elif a == "-files":
            opts["files"] = argv[i + 1].split(",")
            i += 2
        elif a == "-cmdenv":
            k, v = argv[i + 1].split("=", 1)
            opts["env"][k] = v
            i += 2
        elif a in ("-mapper", "-reducer", "-combiner", "-output"):
            opts[a[1:]] = argv[i + 1]
            i += 2
        elif a == "-input":
            opts.setdefault("input", []).append(argv[i + 1])
            i += 2
        else:
            sys.exit("unsupported streaming option %s" % a)

    # Like Hadoop: each -files entry is localized into its own cache folder and
    # symlinked into the task's working folder.
    top = tempfile.mkdtemp(prefix="fakejob_")
    work = os.path.join(top, "task")
    os.makedirs(work)
    for n, f in enumerate(opts["files"]):
        assert os.path.exists(f), "-files entry missing: %s" % f
        cache = os.path.join(top, "filecache", str(n))
        os.makedirs(cache)
        shutil.copy(f, cache)
        os.symlink(os.path.join(cache, os.path.basename(f)), os.path.join(work, os.path.basename(f)))
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(opts["env"])

    inputs = []
    for pat in opts["input"]:
        for p in expand(pat):
            if os.path.isdir(p):
                inputs += [os.path.join(p, n) for n in sorted(os.listdir(p)) if n[0] not in "_."]
            elif os.path.basename(p)[0] not in "_.":
                inputs.append(p)
    assert inputs, "no input files for %s" % opts["input"]

    counters = {}

    def run(cmd, data):
        res = subprocess.run(cmd, shell=True, cwd=work, input=data, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=env)
        for line in res.stderr.decode().splitlines():
            if line.startswith("reporter:counter:"):
                g, n, v = line[len("reporter:counter:"):].split(",")
                counters[(g, n)] = counters.get((g, n), 0) + int(v)
            else:
                sys.stderr.write(line + "\n")
        if res.returncode != 0:
            sys.exit("task failed (%s): exit %d" % (cmd, res.returncode))
        return res.stdout

    def by_key(blob):
        lines = blob.decode().splitlines(keepends=True)
        lines = [l if l.endswith("\n") else l + "\n" for l in lines]
        return sorted(lines, key=lambda l: l.split("\t", 1)[0])

    mapped = []
    for path in inputs:
        out = run(opts["mapper"], open(path, "rb").read())
        if opts["combiner"]:
            out = run(opts["combiner"], "".join(by_key(out)).encode())
        mapped.append(out)
    blob = b"".join(mapped)

    reduces = int(opts["D"].get("mapreduce.job.reduces", "1"))
    parts = [[] for _ in range(reduces)]
    for line in blob.decode().splitlines(keepends=True):
        line = line if line.endswith("\n") else line + "\n"
        parts[zlib.crc32(line.split("\t", 1)[0].encode()) % reduces].append(line)

    outdir = local(opts["output"])
    assert not os.path.exists(outdir), "output directory exists: %s" % opts["output"]
    os.makedirs(outdir)
    for n, lines in enumerate(parts):
        data = "".join(sorted(lines, key=lambda l: l.split("\t", 1)[0])).encode()
        out = run(opts["reducer"], data)
        with open(os.path.join(outdir, "part-%05d" % n), "wb") as fh:
            fh.write(out)
    open(os.path.join(outdir, "_SUCCESS"), "w").close()

    sys.stderr.write("INFO mapreduce.Job: Counters: %d\n" % (len(counters) + 1))
    groups = {}
    for (g, n), v in counters.items():
        groups.setdefault(g, []).append((n, v))
    for g, items in groups.items():
        sys.stderr.write("\t%s\n" % g)
        for n, v in sorted(items):
            sys.stderr.write("\t\t%s=%d\n" % (n, v))
    shutil.rmtree(top)


def main():
    tool, argv = sys.argv[1], sys.argv[2:]
    if tool == "hdfs":
        hdfs(argv)
    elif tool == "hadoop":
        if argv[0] != "jar":
            sys.exit("unsupported hadoop command")
        run_streaming(argv[2:])


if __name__ == "__main__":
    main()
