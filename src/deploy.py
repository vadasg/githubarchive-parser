import glob, os, sys, math, itertools
import time as Time
from datetime import *
from dateutil.parser import *
from dateutil.relativedelta import *

start = Time.time()

def chunk(a, n):
    k, m = len(a) / n, len(a) % n
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def clean():
    os.system('rm -rf /Users/vadasg/research/thinkaurelius/scratch/*/*json')
    os.system('rm -rf /Users/vadasg/research/thinkaurelius/scratch/*/*gz')


os.system('rm -rf /tmp/*txt')
#clean()

fileList = []
current = parse('2012-03-12-22')

while current <datetime.now():
    fileList.append(current.strftime("%Y-%m-%d-X%H").replace('X0','').replace('X','') + '.json')
    current += relativedelta(hours=+1)

fileList = fileList[:6]
threads = 2
tmpDir = '/tmp/'
scratchDir = '/tmp/'

if len(sys.argv) < 3:
    vertexFile = 'parsed_vertices.txt'
    edgeFile = 'parsed_edges.txt'
else:
    [vertexFile, edgeFile] = sys.argv[1:]
    
tempVertexFile = scratchDir + 'temp_' + vertexFile    
tempEdgeFile = scratchDir + 'temp_' + edgeFile    

splitFileList = chunk(fileList,threads)

jobIndex = 0
for s in splitFileList:
    jobFile = tmpDir + 'job_' + str(jobIndex) + '.txt'
    out = ''
    for f in s:
        out += f + '\n'
    fileHandle = open(jobFile,'w')
    fileHandle.write(out[:-1])
    fileHandle.close()
    jobIndex += 1

cmd = 'for i in {0..' + str(threads-1) + '}; do python batch.py $i & done; wait'
#print cmd
os.system(cmd)

cmd = 'cat ' + tmpDir + 'vertices_*.txt > ' + tempVertexFile + ' & '
cmd += 'cat ' + tmpDir + 'edges_*.txt > ' + tempEdgeFile + ' & wait '
#print cmd
os.system(cmd)

print 'sorting output'
cmd = 'time sort -u -S2G -T' + scratchDir + ' -o ' + vertexFile + ' ' + tempVertexFile +' & '
cmd += 'time sort -u -S2G -T' + scratchDir + ' -o ' + edgeFile + ' ' + tempEdgeFile + ' &  wait'
#print cmd
os.system(cmd)

print 'done!'
print (Time.time()-start), 'seconds elapsed'
