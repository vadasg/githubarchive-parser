import groovy.json.JsonSlurper
import com.thinkaurelius.titan.core.*

start = System.currentTimeMillis() 
last = start
def config = [ "cache_type":"none" ] 

graph = TitanFactory.open('../../scratch/gha')
graph.createKeyIndex('name',Vertex.class)
BatchGraph bgraph = new BatchGraph(graph, BatchGraph.IdType.STRING, 10000)
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
            currentObject.setProperty(pair.key+"_",pair.value)
        }
    }
}


def addEdges = { line -> 
    result = slurper.parseText(line)
    result = slurper.parseText(line)
    if (result.type == 'GistEvent') return

    //println result.actor
    
    //out vertex is actor
    outVertex=bgraph.getVertex(result.actor)
    if (outVertex==null) {
        outVertex=bgraph.addVertex(result.actor)
        safePropertyAdder(outVertex, result.actor_attributes) 
    }
    outVertex.setProperty("actor",result.actor)
    //println outVertex

    if (result.type == 'FollowEvent') {
        //in vertex is another user
        inVertex=bgraph.getVertex(result.payload.target.login)
        if (inVertex==null) {
            inVertex=bgraph.addVertex(result.payload.target.login)
        }
        safePropertyAdder(inVertex,result.payload.target)
        //println inVertex
        result.remove("payload")
    }
    else {
        //in vertex is repository
        inVertex=bgraph.getVertex(result.repository.name)
        if (inVertex==null) {
            inVertex=bgraph.addVertex(result.repository.name)
            safePropertyAdder(inVertex,result.repository)
        }
        //println inVertex
        result.remove("repository")
    }
        result.remove("actor")
        result.remove("actor_attributes")
        Edge edge = bgraph.addEdge(null,outVertex,inVertex,result.type)
        //println edge
        safePropertyAdder(edge,result)

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


myFile = new File('../../scratch/githubarchive/2012-09-27-8.json').eachLine {line ->addEdges(line)}
now = System.currentTimeMillis()  

graph.shutdown()

now = System.currentTimeMillis()  
println 'Total elapsed time in ms'
println (now - start)
