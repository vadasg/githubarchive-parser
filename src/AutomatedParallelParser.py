import glob, os, sys, math, itertools
import time as Time
from datetime import *
from dateutil.parser import *
from dateutil.relativedelta import *

#directory for temporary files and logs
#tmpDir = '../../../scratch/tmp/'
tmpDir = '/tmp/'

#directory for scratch space and parsed files
scratchDir =  '../../../scratch/'
gzDir = scratchDir + 'githubarchive_gz/'
jsonDir = scratchDir + 'githubarchive_json/'

#output directory and file names
outDir = scratchDir + 'full/'
vertexFile = 'parsed_vertices.txt'
edgeFile = 'parsed_edges.txt'

#locations of scripts called
rubyScript = 'FixGitHubArchiveDelimiters.rb'
parseScript = 'ParseGitHubArchive.groovy'  

#system specific settings
sortMem = '1500M'   #memory for sort.  should be just under half system memory
threads = 2 

#start and end hours to fetch from GitHubArchive
startHour = '2012-03-12-01'  #set to 'beginning' for earliest possible
#endHour = '2012-11-09-23'    #set to 'now' for last possible
endHour = '2012-03-12-04'    #set to 'now' for last possible


#verbose output for debugging
debug = False

def chunk(a, n):
    k, m = len(a) / n, len(a) % n
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

def clean():
    os.system('rm -rf ' + gzDir + '/*gz')
    os.system('rm -rf ' + jsonDir + '/*json')
    os.system('rm -rf ' + tmpDir + '*txt')

def batchJob(jobIndex):
    verticesFile = tmpDir + 'vertices_' + jobIndex + '.txt'
    edgesFile = tmpDir + 'edges_' + jobIndex + '.txt'
    jobFile = tmpDir + 'job_' + jobIndex + '.txt'
    logFile = tmpDir + 'log_' + jobIndex + '.txt'

    gzFiles = glob.glob(gzDir + '*gz')
    jsonFiles = glob.glob(jsonDir + '*json')

    files = open(jobFile,'r').read().split('\n')
    out = ''
    for f in files:
        out += jsonDir + f +'\n'
    fileHandle = open(jobFile.replace('.txt','_full.txt'),'w')
    fileHandle.write(out[:-1])
    fileHandle.close()

    for f in files:
        if gzDir + f + '.gz' not in gzFiles:
            print 'downloading ' + f
            cmd = 'wget -q -nc -O ' 
            cmd += gzDir + f + '.gz '
            cmd += 'http://data.githubarchive.org/' + f + '.gz'
            os.system(cmd)

        if jsonDir + f not in jsonFiles:
            print 'cleaning ' + f
            cmd = 'ruby1.9 ' + rubyScript + ' ' + gzDir + f + '.gz ' + jsonDir + f
            os.system(cmd)

    print 'parsing job ' + jobIndex
    cmd = 'groovy ' + parseScript + ' ' + jobFile.replace('.txt','_full.txt') 
    cmd += ' ' + verticesFile + ' ' + edgesFile + ' > ' + logFile
    os.system(cmd)

def deploy():
    ensure_dir(gzDir)
    ensure_dir(jsonDir)
    ensure_dir(tmpDir)
    ensure_dir(scratchDir)
    ensure_dir(outDir)


    start = Time.time()

    fileList = []

    if startHour == 'beginning':
        current = parse('2012-03-10-22')
    else:
        current = parse(startHour)

    if endHour == 'now':
        end = datetime.now()
    else:
        end =  parse(endHour)

    while current < end:
        fileList.append(current.strftime("%Y-%m-%d-X%H").replace('X0','').replace('X','') + '.json')
        current += relativedelta(hours=+1)

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
    

    cmd = 'for i in {0..' + str(threads-1) + '}; do python ' + sys.argv[0] + ' job $i & done; wait'
    if debug: print cmd
    os.system(cmd)

    tempVertexFile = scratchDir + vertexFile.replace('.txt','_temp.txt')
    tempEdgeFile = scratchDir + edgeFile.replace('.txt','_temp.txt')
    
    cmd = 'cat ' + tmpDir + 'vertices_*.txt > ' + tempVertexFile + ' & '
    cmd += 'cat ' + tmpDir + 'edges_*.txt > ' + tempEdgeFile + ' & wait '
    if debug: print cmd
    os.system(cmd)

    print 'sorting output'
    cmd = 'time sort -u -S' + sortMem + ' -T' + scratchDir + ' -o ' + outDir +vertexFile + ' ' + tempVertexFile +' & '
    cmd += 'time sort -u -S' + sortMem + ' -T' + scratchDir + ' -o ' + outDir +edgeFile + ' ' + tempEdgeFile + ' &  wait'
    if debug: print cmd
    os.system(cmd)

    print 'done!'
    print (Time.time()-start), 'seconds elapsed'




if __name__ == '__main__':
    if 'batch' in sys.argv:
        deploy()
    elif 'job' in sys.argv:
        jobIndex = sys.argv[2]
        batchJob(jobIndex)
    elif 'clean' in sys.argv:
        clean()
    else:
        print """Usage:
        $ python AutomatedParallelParser.py <command>
        commands:
        clean\t\tdelete all temporary files for a fresh start
        batch\t\tstart batch processing
        job <jobIndex>\trun job (called during batch processing)
        """

