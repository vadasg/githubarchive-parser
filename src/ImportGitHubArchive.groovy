import com.thinkaurelius.titan.core.*
import org.apache.commons.configuration.BaseConfiguration


/** 
 * Reads in triplet files
 * and loads into Titan graph
 * @author Vadas Gintautas
 *
 *
 * Configuration option are here 
 */

useHBase = true  //if false, use BerkleyDB instead
graphLocation = './gha-graph' //only for BerkleyDB

//get inputFolder as command line argument
try {
    inputVerticesFile = a1
    inputEdgesFile = a2
}
catch (MissingPropertyException) {
    throw new IllegalArgumentException('\n\nusage: gremlin -e ImportGitHubArchive.groovy <inputVertices> <inputEdges>\n')
}



class Counter {
    private int myCount
    private double now
    private double last

    public Counter(start){
        this.myCount = 0
        this.last = start
    }
    
    public void increment() {
        this.myCount++
        if (this.myCount % 1000000 == 0){
            now = System.currentTimeMillis() 
            println this.myCount + ' in ' + ((now - this.last)/1000.0) + ' seconds'
            this.last = now
        }
    }
    public void reset() {
        this.myCount = 0
    }
    public int getCount() {
        return this.myCount
    }
}

//parses triple
def vertexAdder = {g, counter, line ->
    line = line.split('\t')
    if (line.size() < 3) return

    //vertexId = line[0]
    //assert(line[1].startsWith('_'))
    //key = line[1][1..-1]
    //value = line[2]
    
    if (! line[0].equals(lastVertexId))  {
        vertex=g.addVertex(line[0])
        lastVertexId = line[0]
        lastVertex = vertex
        counter.increment()
    }
    lastVertex.setProperty(line[1][1..-1],line[2])
}

//parses triple
def edgeAdder = {g, counter, line ->
    line = line.split('\t')
    if (line.size() < 3){
        return
    }
    outVertexId = line[0]
    label = line[1]
    inVertexId = line[2]
    edge = g.addEdge(null,g.getVertex(outVertexId),g.getVertex(inVertexId),label)
    if (line.size() > 3 ){
        for (l in line[3..-1]) edge.setProperty(l.split('=')[0], l.split('=')[1])
    }
    counter.increment()
}




conf = new BaseConfiguration()

if (useHBase){
    conf.setProperty('storage.backend','hbase')
    conf.setProperty('storage.hostname','localhost')
    conf.setProperty('persist-attempts',10)
    conf.setProperty('persist-wait-time',400)
    conf.setProperty('storage.lock-retries',10)
    conf.setProperty('storage.idauthority-block-size',20000)
    conf.setProperty('storage.idauthority-retries',20)

}else{  //use BerkeleyDB

    graphFile = new File(graphLocation)
    if (graphFile.exists()) assert graphFile.deleteDir()
    assert graphFile.mkdir()

    conf.setProperty('storage.backend','local')
    //conf.setProperty('storage.cache_percentage','85')
    conf.setProperty('storage.directory',graphLocation)
    //conf.setProperty('buffer-size',1024)
}



conf.setProperty('storage.transactions','false')
conf.setProperty("storage.batch-loading","true")
graph = TitanFactory.open(conf)
graph.createKeyIndex('name',Vertex.class)

bgraph = new BatchGraph(graph, BatchGraph.IdType.STRING, 10000)
bgraph.setLoadingFromScratch(true)

start = System.currentTimeMillis() 
counter = new Counter(start)
lastVertexId = null
lastVertex = null

try{
    println 'Loading vertices'
    myFile = new File(inputVerticesFile).eachLine {line -> vertexAdder(bgraph,counter, line)}
    vertexCount = counter.getCount()
    counter.reset()
    println 'Loading edges'
    myFile = new File(inputEdgesFile).eachLine {line ->edgeAdder(bgraph, counter, line)}
    edgeCount = counter.getCount()
} finally{
    //sun.management.ManagementFactory.getDiagnosticMXBean().dumpHeap('dump.bin', true)
    bgraph.shutdown()
    graph.shutdown()
}



now = System.currentTimeMillis()  
elapsed =  ((now - start)/1000.0)
println 'Done.  Statistics:'
println vertexCount + ' vertices'
println edgeCount + ' edges'
println elapsed + ' seconds elapsed'
