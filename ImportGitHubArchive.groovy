import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import com.thinkaurelius.titan.core.*

/** 
 * Reads in uncompressed githubarchive json files in a specified directory
 * and loads into Titan graph
 * @author Vadas Gintautas
 *
 *
 * Configuration option are here 
 */


useHBase = true  //if false, use BerkleyDB instead
graphLocation = '../../scratch/debug_graph' //only for BerkleyDB


//get inputFolder as command line argument
try {
    inputVerticesFile = a1
    inputEdgesFile = a2
}
catch (MissingPropertyException) {
    throw new IllegalArgumentException('\n\nusage: gremlin -e ImportGitHubArchive.groovy <inputVertices> <inputEdges>\n')
}



slurper = new JsonSlurper()
start = System.currentTimeMillis() 
last  = System.currentTimeMillis() 
vertexCount = 0
edgeCount = 0

def counter= {type ->
    if (type.equals('vertex')) vertexCount = vertexCount + 1
    else if (type.equals('edge')) edgeCount = edgeCount + 1



    if ((vertexCount > 0) && (vertexCount % 1000000).equals(0)){
        now = System.currentTimeMillis()
        elapsed = ((now - last)/1000.0)
        last = now
        println vertexCount + ' vertices in ' + elapsed + ' seconds'
    }
    if ((edgeCount > 0) && (edgeCount  % 1000000 ).equals(0)){ 
        now = System.currentTimeMillis()
        elapsed = ((now - last)/1000.0)
        last = now
        println edgeCount + ' edges in ' + elapsed + ' seconds'
    }
}


def safePropertyAdder = {currentObject, propertyMap ->
    for (pair in propertyMap) {
        if (pair.value == null) return
        if (pair.key in ['type','name','id','label']){
            currentObject.setProperty('github_' + pair.key,pair.value)
        } else  currentObject.setProperty(pair.key,pair.value)
        
    }
}


def vertexAdder = {g, s, line ->

    vertexId = line.substring(0, line.indexOf('\t'))
    propertiesString = line.substring(line.indexOf('\t')+1,line.size())
    name = vertexId.reverse().substring(vertexId.reverse().indexOf('_')+1,vertexId.size()).reverse()
    type = vertexId.reverse().substring(0,vertexId.reverse().indexOf('_')).reverse()
    if (propertiesString.equals('null')) propertiesString = '{}'
    properties = s.parseText(propertiesString)


    if (name==null) throw new IllegalArgumentException('Name cannot be null')
    if (type==null) throw new IllegalArgumentException('Type cannot be null')

    vertex=g.addVertex(vertexId)
    safePropertyAdder(vertex,properties)
    vertex.setProperty('name',name)
    vertex.setProperty('type',type)
    counter('vertex')
    
}


def edgeAdder = {g, s, line ->
    firstTab = line.indexOf('\t')
    secondTab = line.indexOf('\t',firstTab+1)
    thirdTab = line.indexOf('\t',secondTab+1)

    outVertex = line.substring(0,firstTab)
    inVertex = line.substring(firstTab+1,secondTab)
    label = line.substring(secondTab+1,thirdTab)
    propertiesString = line.substring(thirdTab+1,line.size())
    if (propertiesString.equals('null')) propertiesString = '{}'
    properties = s.parseText(propertiesString)

    if (label==null) throw new IllegalArgumentException('Label cannot be null')
    
    edge = g.addEdge(null,g.getVertex(outVertex),g.getVertex(inVertex),label)
    safePropertyAdder(edge,properties)
    counter('edge')
}



last = start
def config = [ 'cache_type':'none' ] 

conf = new BaseConfiguration()
if (useHBase){
    conf.setProperty("storage.backend","hbase")
    conf.setProperty('storage.hostname','localhost')
}else{
    conf.setProperty("storage.backend","local")
    conf.setProperty("storage.directory",graphLocation)
}


conf.setProperty("storage.batch-loading","true")

graph = TitanFactory.open(conf)
println 'ok'

graph.createKeyIndex('name',Vertex.class)
BatchGraph bgraph = new BatchGraph(graph, BatchGraph.IdType.STRING, 20000)
bgraph.setLoadingFromScratch(true)

println 'Loading vertices'
myFile = new File(inputVerticesFile).eachLine {line ->vertexAdder(bgraph, slurper, line)}
println 'Loading edges'
myFile = new File(inputEdgesFile).eachLine {line ->edgeAdder(bgraph, slurper, line)}

graph.shutdown()



now = System.currentTimeMillis()  
elapsed =  ((now - start)/1000.0)
println 'Done.  Statistics:'
println vertexCount + ' vertices'
println edgeCount + ' edges'
println elapsed + ' seconds elapsed'
