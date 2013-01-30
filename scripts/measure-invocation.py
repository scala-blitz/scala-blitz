#!/usr/bin/python

import subprocess
import sys
import tempfile
from itertools import groupby
from cStringIO import StringIO


if len(sys.argv) < 2:
    print "Must provide SBT command name!"
    sys.exit(1)


parlevels = [1, 2, 4, 8]
configs = [
    ("PollingStarter", 100000, "interruptible", "scala.collection.parallel.Loop"),
    ("PollingStarter", 100000, "interruptible", "scala.collection.parallel.WorkstealingScheduler"),
    ("NotifyStarter", 100000, "interruptible", "scala.collection.parallel.WorkstealingScheduler"),
    ("NotifyStarter", 0, "interruptible", "scala.collection.parallel.WorkstealingScheduler"),
    ("NotifyStarter", 100000, "direct", "scala.collection.parallel.WorkstealingScheduler")
]
sizes = [
    (150000000, 1),
    (15000000, 10),
    (1500000, 100),
    (150000, 1000),
    (15000, 10000),
    (1500, 100000),
    (150, 1000000)
]
step = 1
maxStep = 1024
results = {}
marks = {}
for k, v in zip(configs, ["", "o", "triangle", "square", "x"]):
    marks[k] = v
names = {}
for k, v in zip(configs, ["baseline", "poll", "poll/monitor", "monitor", "direct"]):
    names[k] = v


def average(list):
    return float(sum(list)) / float(len(list))


def bench(config, par, size):
    bname = config[3]
    sbtcommand = sys.argv[1]
    output = tempfile.TemporaryFile()
    command = [sbtcommand, "-Dkernel=uniform", "bench -Dpar=" + str(par) + " -Dblock=1024 -Dstep=" + str(step) + " -DmaxStep=" + str(maxStep) + " -Dsize=" + str(size[0]) + " -Drepeats=" + str(size[1]) + " -Dstrategy=FindMax$ " + " -DinvocationMethod=" + config[2] + " -DstarterThread=" + config[0] + " -DstarterCooldown=" + str(config[1]) + " " + bname + " 15"]
    print command
    retcode = subprocess.call(command, stdout = output)
    output.seek(0)
    for line in output:
        if line.startswith(bname):
            print line
            trimmed = [int(n.strip()) for n in line[len(bname) + 1:].split("\t") if n.strip() != ''][5:]
            results[(config, par, size)] = trimmed
    output.close()


for size in sizes:
    print "size, repeats: %d, %d" % (size[0], size[1])
    for par in parlevels:
        print "parallelism level: %d" % (par)
        for config in configs:
            print "config: %s, %d, %s, %s" % (config[0], config[1], config[2], config[3])
            bench(config, par, size)


perpar={}
for parlevel, lst in [(parlevel, list(g)) for parlevel, g in groupby(sorted(results, key = lambda t: t[1]), lambda t: t[1])]:
    perpar[parlevel] = lst
for par in sorted(perpar, key = lambda t: t):
    curves = [(curvename, list(g)) for curvename, g in groupby(sorted(perpar[par], key = lambda t: t[0]), lambda t: t[0])]
    buf = StringIO()
    print >>buf, "\\begin{tikzpicture}[scale=0.5]"
    if par < 4:
        print >>buf, "\\begin{semilogxaxis}[height=6cm,width=6cm,legend style={at={(0.95,0.05)},anchor=south east}]"
    else:
        print >>buf, "\\begin{semilogxaxis}[height=6cm,width=6cm,legend style={at={(0.05,0.95)},anchor=north west}]"
    print >>buf, "\\addlegendimage{legend image code/.code=}"
    print >>buf, "\\addlegendentry{\\tiny{$P = %d$}}" % (par)
    for curvename, points in curves:
        print >>buf, "\\addplot[solid,mark=%s]" % (marks[curvename])
        print >>buf, "plot coordinates {"
        for p in sorted(points, key = lambda t: t[2]):
            print >>buf, "(%d, %f)" % (p[2][0], 1000 / average(results[p]))
        print >>buf, "};"
        print >>buf, "\\addlegendentry{\\tiny{%s}}" % (names[curvename])
    print >>buf, "\end{semilogxaxis}"
    print >>buf, "\end{tikzpicture}"
    print buf.getvalue()













