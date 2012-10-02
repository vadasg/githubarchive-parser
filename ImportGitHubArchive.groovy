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
        println pair
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

        println 'just checking at edge 1'
        edge = bgraph.addEdge(null,outVertex,inVertex,name)
        println 'just checking at edge 2'
        safePropertyAdder(edge,properties)
        return edge
}


def loader = { line -> 
    s = slurper.parseText(line)
    s = slurper.parseText(line)
    println s.actor
    println s.type
    //
    //out vertex is always a user
    //name is username or login
    outVertex=addVertex(s.actor,['actor':s.actor]+s.actor_attributes)
    s.remove('actor')
    s.remove('actor_attributes')

    switch (s.type.toString()){

        case 'GistEvent':

            inVertex = addVertex(s.payload.name, s.remove('payload'))
            addEdge(outVertex,inVertex,'created',s)
            break

        case 'FollowEvent':
            
            inVertex = addVertex(s.payload.target.login, s.remove('payload').remove('target'))
            addEdge(outVertex,inVertex,'followed',s)
            break

        case ['MemberEvent','TeamAddEvent']:
            
            inVertex1 = addVertex(s.payload.target.login, s.remove('payload').remove('target'))
            inVertex2 = addVertex(s.repository.name,s.remove('repository'))
            addEdge(outVertex,inVertex1,'added',s)
            addEdge(inVertex1,inVertex2,'to',s)
            break

        case 'CommitCommentEvent':
            return
            inVertex1 = addVertex(s.payload.comment_id, s.remove('payload'))
            inVertex2 = addVertex(s.repository.name,s.remove('repository'))
            println 'just checking'
            println s
            addEdge(outVertex,inVertex1,'created',s)
            //addEdge(inVertex1,inVertex2,'on',s)
            break

        case 'IssuesEvent':
            
            inVertex1 = addVertex(s.payload.issue,s.payload)
            inVertex2 = addVertex(s.repository.name,s.remove('repository'))
            addEdge(outVertex,inVertex1,'created',s)
            addEdge(inVertex1,inVertex2,'on',s)
            break

        case 'IssuesCommentEvent':
            
            inVertex1 = addVertex(s.payload.comment_id, [:])
            inVertex2 = addVertex(s.payload.issue_id,[:])
            inVertex3 = addVertex(s.repository.name,s.remove('repository'))
            addEdge(outVertex,inVertex1,'created',s)
            addEdge(inVertex1,inVertex2,'on',s)
            addEdge(inVertex2,inVertex3,'on',s)
            break

        case 'PullRequestReviewCommentEvent':
            
            inVertex1 = addVertex(s.payload.comment.comment_id, s.remove('payload'))
            inVertex2 = addVertex(s.repository.name,s.remove('repository'))
            addEdge(outVertex,inVertex1,'created',s)
            addEdge(inVertex1,inVertex2,'on',s)
            break

        case [ 'CreateEvent', 'WatchEvent', 'DownloadEvent', 'DeleteEvent', 'ForkEvent', 'ForkApplyEvent', 'GollumEvent', 'PublicEvent', 'PullRequestEvent' ]:

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
            inVertex = addVertex(s.repository.name,s.remove('repository'))
            addEdge = addEdge(outVertex,inVertex,edgeNameMap[s.type],s)
            break
        

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


//for (fileName in files){
//    println fileName
//    myFile = new File(fileName.toString()).eachLine {line ->addEdges(line)}
//}


myFile = new File('../../scratch/githubarchive/2012-09-27-8.json').eachLine {line ->loader(line)}
now = System.currentTimeMillis()  

graph.shutdown()

now = System.currentTimeMillis()  
println 'Total elapsed time in ms'
println (now - start)
