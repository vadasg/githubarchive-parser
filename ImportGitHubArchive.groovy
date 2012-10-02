import groovy.json.JsonSlurper
import com.thinkaurelius.titan.core.*


start = System.currentTimeMillis() 
last = start
def config = [ 'cache_type':'none' ] 

graph = TitanFactory.open('../../scratch/gha')
graph.createKeyIndex('name',Vertex.class)
BatchGraph bgraph = new BatchGraph(graph, BatchGraph.IdType.OBJECT, 10000)
slurper = new JsonSlurper()

count = 0


//written as function instead of closure due to recursion
def safePropertyAdder(currentObject, propertyMap){
    for (pair in propertyMap) {
        if (pair.value == null) return
        try{
            if (pair.value.getClass().equals(java.util.HashMap)){
                //process maps recursively
                safePropertyAdder(currentObject,pair.value)
            }
            else {
                currentObject.setProperty(pair.key,pair.value)
            }
        }
        catch(ScriptException){  
            //needed if the key is "id" or "label" or some other reserved word
            currentObject.setProperty(pair.key+'_',pair.value)
        }
    }
}

def addVertex = { name, properties ->

            vertex=bgraph.getVertex(name)
            if (vertex==null) {
                vertex=bgraph.addVertex(name)
                safePropertyAdder(vertex,properties)
            }
            return vertex
}

def addEdge = {inVertex, outVertex, name, properties->

        edge = bgraph.addEdge(null,outVertex,inVertex,name)
        safePropertyAdder(edge,properties)
        return edge
}


def loader = { line -> 
    s = slurper.parseText(line)
    //println s
    if (s.actor == null) return
    
    //out vertex is always a user
    //name is login
    lastVertex=addVertex(s.actor,['actor':s.remove('actor')]+s.remove('actor_attributes'))
    

    switch (s.type){

        case 'GistEvent':
            vertexNames = [s.payload.name]
            vertexProperties = [s.remove('payload')]
            edgeNames = ['created']
            edgeProperties = [s]
            break

        case 'FollowEvent':
            
            vertexNames = [s.payload.target.login]
            vertexProperties = [s.remove('payload').remove('target')]
            edgeNames = ['followed']
            edgeProperties = [s]
            break

        case ['MemberEvent','TeamAddEvent']:

            vertexNames = [s.payload.member.login,s.repository.name]
            vertexProperties = [s.remove('payload').remove('target'),s.remove('repository')]
            edgeNames = ['added','to']
            edgeProperties = [s,s]
            break

        case 'CommitCommentEvent':
            
            vertexNames = [s.payload.comment_id,s.repository.name]
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeNames = ['created','on']
            edgeProperties = [s,s]
            break

        case 'IssuesEvent':
            vertexNames = [s.payload.issue,s.repository.name]
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeNames = ['created','on']
            edgeProperties = [s,s]

            break

        case 'IssueCommentEvent':
            
            vertexNames = [s.payload.comment_id,s.payload.issue_id,s.repository.name]
            vertexProperties = [[:],[:],s.remove('repository')]
            edgeNames = ['created','on','on']
            edgeProperties = [s,s,s]
            break

        case 'PullRequestReviewCommentEvent':
            
            vertexNames = [s.payload.comment.commit_id,s.repository.name]
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeNames = ['created','on']
            edgeProperties = [s,s]
            break

        case [ 'PushEvent','CreateEvent', 'WatchEvent', 'DownloadEvent', 'DeleteEvent', 'ForkEvent', 'ForkApplyEvent', 'GollumEvent', 'PublicEvent', 'PullRequestEvent' ]:

            edgeNameMap = [
                'CreateEvent':'created',
                'WatchEvent':'watched',
                'DownloadEvent':'downloaded',
                'DeleteEvent':'deleted',
                'ForkEvent':'forked',
                'ForkApplyEvent':'appliedForkTo',
                'GollumEvent':'editedWikiOf',
                'PublicEvent':'madePublic',
                'PullRequestEvent':'pullRequested',
                'PushEvent':'pushed'
            ]

            vertexNames = [s.repository.name]
            vertexProperties = [s.remove('repository')]
            edgeNames = [edgeNameMap[s.type]]
            edgeProperties = [s]
            break
        

    }

    for (i in 0..vertexNames.size()-1) {
        nextVertex = addVertex(vertexNames[i],vertexProperties[])
        addEdge(lastVertex,nextVertex,edgeNames[i],edgeProperties[i])
        lastVertex = nextVertex
    }

    count = count + 1
    if (count % 1000000 == 0 ) { 
        now = System.currentTimeMillis()
        sec = (now - last)
        println sec.toString() +  ' ms for ' + count.toString()
        last = now
    }
    
}




folder = '../../scratch/githubarchive'
baseDir = new File(folder)
files = baseDir.listFiles()

/*
For now, assume that input files contain one json entry per line.  Some of the early files in the archive do not have newline delimiters between entries and have to be fixed using a separate script.  This can be streamlined later.
*/


for (fileName in files){
    println fileName
    myFile = new File(fileName.toString()).eachLine {line ->loader(line)}
}


//myFile = new File('../../scratch/githubarchive/2012-03-11-10.json').eachLine {line ->loader(line)}
now = System.currentTimeMillis()  

graph.shutdown()

now = System.currentTimeMillis()  
println 'Total elapsed time in ms'
println (now - start)
