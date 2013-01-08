#!/usr/bin/python

import subprocess
import sys
import tempfile
from itertools import groupby
from collections import defaultdict


if len(sys.argv) < 2:
	print "Must provide SBT command name!"
	sys.exit(1)


loopsimple = "scala.collection.parallel.Loop"
loopblocks = "scala.collection.parallel.BlockedSelfSchedulingLoop"
loopwstree = "scala.collection.parallel.StealLoop"
parlevels = [1, 2, 4, 8]
steps = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 50, 52, 56, 60, 64, 72, 80, 88, 92, 100, 108, 116, 124, 128, 144, 160, 176, 192, 224, 256, 320, 384, 512, 768, 1024, 1280, 1536, 1792, 2048]
results = {}


def average(list):
	return float(sum(list)) / float(len(list))


def bench(bname, par, step):
    sbtcommand = sys.argv[1]
    output = tempfile.TemporaryFile()
    retcode = subprocess.call([sbtcommand, "bench -Dpar=" + str(par) + " -Dstep=" + str(step) + " -Dsize=150000000 -Dstrategy=Predefined$ " + bname + " 35"], stdout = output)
    output.seek(0)
    for line in output:
    	if line.startswith(bname):
    		trimmed = [int(n.strip()) for n in line[len(bname) + 1:].split("\t") if n.strip() != ''][5:]
    		results[(bname, par, step)] = trimmed
    output.close()


bench(loopsimple, 0, 0)
for par in parlevels:
	print "Parallelism level: %d" % (par)
	for step in steps:
		print "Step: %d" % (step)
		bench(loopblocks, par, step)
		bench(loopwstree, par, step)


pargroups = [(p, list(g)) for p, g in groupby(sorted(results, key = lambda t: t[1]), lambda k: k[1])]
for pargroup in pargroups:
	if pargroup[0] != 0:
		namegroups = {}
		for k, v in [(name, list(g)) for name, g in groupby(sorted(pargroup[1], key = lambda t: t[0]), lambda t: t[0])]:
			namegroups[k] = v
		resblocks = namegroups[loopblocks]
		reswstree = namegroups[loopwstree]
		print "\\begin{tikzpicture}[scale=0.67]"
		print "\\begin{loglogaxis}[xlabel=\\texttt{P=%d},height=5cm,width=5cm]" % (pargroups[1])
		print "\\addplot[smooth,mark=x]"
		print "plot coordinates {"
		for t in sorted(resblocks, key = lambda t: t[2]):
			print "(%d, %d)" % (t[2], average(results[t]))
		print "};"
		print "\\addplot[smooth,mark=o]"
		print "plot coordinates {"
		for t in sorted(reswstree, key = lambda t: t[2]):
			print "(%d, %d)" % (t[2], average(results[t]))
		print "};"
		print "\\addplot[smooth]"
		print "plot coordinates {"
		for step in steps:
			print "(%d, %f)" % (step, average(results[(loopsimple, 0, 0)]))
		print "};"
		print "\end{loglogaxis}"
		print "\end{tikzpicture}"











