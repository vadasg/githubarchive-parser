import glob,os,sys

def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

jobIndex = sys.argv[1]

tmpDir = '../../../scratch/tmp/'
scratchDir =  '../../../scratch/'
gzDir = scratchDir + 'gha/githubarchive_gz/'
jsonDir = scratchDir + 'gha/githubarchive_json/'
rubyScript = 'FixGitHubArchiveDelimiters.rb'
parseScript = 'ParseGitHubArchive.groovy'
gremlinSh = '../../titan-mod/bin/gremlin.sh'
verticesFile = tmpDir + 'vertices_' + jobIndex + '.txt'
edgesFile = tmpDir + 'edges_' + jobIndex + '.txt'

jobFile = tmpDir + 'job_' + jobIndex + '.txt'
files = open(jobFile,'r').read().split('\n')

out = ''
for f in files:
    out += jsonDir + f +'\n'
fileHandle = open(jobFile.replace('.txt','_full.txt'),'w')
fileHandle.write(out[:-1])
fileHandle.close()

logFile = tmpDir + 'log_' + jobIndex + '.txt'


ensure_dir(gzDir)
ensure_dir(jsonDir)

gzFiles = glob.glob(gzDir + '*gz')
jsonFiles = glob.glob(jsonDir + '*json')

for f in files:
    if gzDir + f + '.gz' not in gzFiles:
        print 'downloading ' + f
        cmd = 'wget -q -nc -O ' 
        cmd += gzDir + f + '.gz '
        cmd += 'http://data.githubarchive.org/' + f + '.gz'
        os.system(cmd)

    if jsonDir + f not in jsonFiles:
        print 'cleaning ' + f
        cmd = 'ruby ' + rubyScript + ' ' + gzDir + f + '.gz ' + jsonDir + f
        os.system(cmd)

print 'parsing job ' + jobIndex
cmd = gremlinSh + ' -e ' + parseScript + ' ' + jobFile.replace('.txt','_full.txt') + ' ' + verticesFile + ' ' + edgesFile + ' > ' + logFile
os.system(cmd)
