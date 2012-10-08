import groovy.json.JsonSlurper
import com.thinkaurelius.titan.core.*

/** 
 * Reads in gzipped githubarchive json files in a specified directory
 * and loads into Titan graph
 * @author Vadas Gintautas
 */


debug = false
verbose = false
slurper = new JsonSlurper()
count = 0

/**
 * Written as function instead of closure due to recursion
 */
def safePropertyAdder(currentObject, propertyMap){
    for (pair in propertyMap) {
        if (pair.value == null) return
        if (pair.key in ['type','name','id','label']){
            currentObject.setProperty('github_' + pair.key,pair.value)
        } else  currentObject.setProperty(pair.key,pair.value)
        
    }
}


def vertexAdder = {g,  name, type, properties ->
    if (name==null) throw new IllegalArgumentException('Name cannot be null')
    if (name==null) throw new IllegalArgumentException('Type cannot be null')

    vertex=g.getVertex(name)
    if (vertex==null) {
        vertex=g.addVertex(name)
        safePropertyAdder(vertex,properties)
        vertex.setProperty('name',name)
        vertex.setProperty('type',type)
    }
    return vertex
}


def edgeAdder = {g,outVertex, inVertex, name, properties->
    if (name==null) throw new IllegalArgumentException('Name cannot be null')
    
    edge = g.addEdge(null,outVertex,inVertex,name)
    safePropertyAdder(edge,properties)
    edge.setProperty('name',name)
    return edge
}


def loader = {g, line -> 






    s = slurper.parseText(line)
    if (s.actor == null) return

    if (verbose) {
        println s
        println s.actor
        println s.type
    }
    
    /**
     * Out vertex is always a user.
     * Occasionally actor_attributes is missing so we allow for this.
     */
    actorProperties = ['actor':s.actor]
    if (s.actor_attributes != null) actorProperties = actorProperties +  s.remove('actor_attributes')
    lastVertex=vertexAdder(g, s.remove('actor'),'User',actorProperties)
            
            
    if (s.repository != null) repoOrg = s.repository.organizatio
    
    edgeNameMap = [
        'CreateEvent':'created',
        'WatchEvent':'watched',
        'DownloadEvent':'downloaded',
        'DeleteEvent':'deleted',
        'ForkEvent':'forked',
        'ForkApplyEvent':'appliedForkTo',
        'PublicEvent':'madePublic',
        'PullRequestEvent':'pullRequested',
    ]

    switch (s.type){

        /**
         * GistEvent: User created Gist.
         */
        case 'GistEvent':
        
            vertexNames = [s.payload.name]
            vertexTypes = ['Gist']
            vertexProperties = [s.remove('payload')]
            edgeNames = ['created']
            edgeProperties = [s]
            break
        
        /**
         * FollowEvent: User followed User.
         */
        case 'FollowEvent':
            
            vertexNames = [s.payload.target.login]
            vertexTypes = ['User']
            vertexProperties = [s.remove('payload').remove('target')]
            edgeNames = ['followed']
            edgeProperties = [s]
            break

        /**
         * MemberEvent, TeamAddEvent: User added User to Repository.
         * No TeamAddEvents observed in entire archive to date.
         * Occasionally repository is missing (here and following cases).
         * If so, ignore the event.
         */
        case ['MemberEvent','TeamAddEvent']:
            if (s.repository == null) return

            vertexNames = [s.payload.member.login,s.repository.name]
            vertexTypes = ['User','Repository']
            vertexProperties = [s.remove('payload').remove('target'),s.remove('repository')]
            edgeNames = ['added','to']
            edgeProperties = [s,s]
            break

        /**
         * CommitCommentEvent: User created Comment on Repository.
         */
        case 'CommitCommentEvent':
            if (s.repository == null) return
            
            vertexNames = [s.payload.comment_id,s.repository.name]
            vertexTypes = ['Comment','Repository']
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeNames = ['created','on']
            edgeProperties = [s,s]
            break

        /**
         * IssuesEvent: User created Issue on Repository.
         */
        case 'IssuesEvent':
            if (s.repository == null) return

            vertexNames = [s.payload.issue,s.repository.name]
            vertexTypes = ['Issue','Repository']
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeNames = ['created','on']
            edgeProperties = [s,s]

            break
        
        /**
         * GollumEvent: User edited WikiPage on Repository.
         */
        case 'GollumEvent':
            if (s.repository == null) return

            pageNames = []
            pageProperties = []
            pageTypes = []
            pageEdgeNames = []
            pageEdgeProperties = []

            for (p in s.payload.pages){
                pageNames.add(p.remove('html_url'))
                pageProperties.add(p)
                pageTypes.add('WikiPage')
                pageEdgeNames.add('edited')
                pageEdgeProperties.add([:])
            }
            

            vertexNames = [pageNames,s.repository.name]
            vertexTypes = [pageTypes,'Repository']
            vertexProperties = [pageProperties, s.remove('repository')]
            edgeNames = [pageEdgeNames,'on']
            edgeProperties = [pageEdgeProperties,s]

            break

        /**
         * PushEvent: User pushed commit on Repository.
         */
        case 'PushEvent':
            if (s.repository == null) return
            if (s.payload.shas.size() == 0) return

            pageNames = []
            pageProperties = []
            pageTypes = []
            pageEdgeNames = []
            pageEdgeProperties = []

            for (p in s.payload.shas){
                pageNames.add(p[0])
                pageProperties.add(['payload':p.drop(1)])
                pageTypes.add('Commit')
                pageEdgeNames.add('pushed')
                pageEdgeProperties.add(['created_at':s.created_at,'public':s.public,'url':s.url])
            }

            s.remove('payload')
            

            vertexNames = [pageNames,s.repository.name]
            vertexTypes = [pageTypes,'Repository']
            vertexProperties = [pageProperties, s.remove('repository')]
            edgeNames = [pageEdgeNames,'on']
            edgeProperties = [pageEdgeProperties,s]

            break
        
        
        /**
         * IssuesCommentEvent: User created Comment on Issue on Repository.
         */
        case 'IssueCommentEvent':
            if (s.repository == null) return
            
            vertexNames = [s.payload.comment_id,s.payload.issue_id,s.repository.name]
            vertexTypes = ['Comment','Issue','Repository']
            vertexProperties = [[:],[:],s.remove('repository')]
            edgeNames = ['created','on','on']
            edgeProperties = [s,s,s]
            break

        /**
         * PullRequestReviewCommentEvent: User created Comment on Repository.
         */
        case 'PullRequestReviewCommentEvent':
            if (s.repository == null) return
            
            vertexNames = [s.payload.comment.commit_id,s.repository.name]
            vertexTypes = ['Comment','Repository']
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeNames = ['created','on']
            edgeProperties = [s,s]
            break


        /**
         * All other valid cases:  User -- ... --> Repository
         */
        case edgeNameMap.keySet() as List:
            if (s.repository == null) return

            vertexNames = [s.repository.name]
            vertexTypes = ['Repository']
            vertexProperties = [s.remove('repository')]
            edgeNames = [edgeNameMap[s.type]]
            edgeProperties = [s]
            break
        

    }

    count = count + 1
    if (count % 1000000 == 0 ) { 
        now = System.currentTimeMillis()
        sec = (now - last)/1000
        println sec.toString() +  ' s for ' + count.toString()
        last = now
    }

    for (i in 0..vertexNames.size()-1) {
        //here we add a vertex for Organizition and corresponding edges
        if ((vertexTypes[i] == 'Repository') 
            && (repoOrg != null)){
            repositoryVertex = vertexAdder(g,repoOrg,'Organization',[:])
            edgeAdder(g,nextVertex,repositoryVertex,'owns',[:])
        }


        if (vertexNames[i].getClass() == java.util.ArrayList){
            endVertex = vertexAdder(g, vertexNames[i+1],vertexTypes[i+1],vertexProperties[i+1])
            for (j in 0..vertexNames[i].size()-1){
                midVertex = vertexAdder(g, vertexNames[i][j],vertexTypes[i][j],vertexProperties[i][j])
                edgeAdder(g,lastVertex,midVertex,edgeNames[i][j],edgeProperties[i][j])
                edgeAdder(g,midVertex,endVertex,edgeNames[i][j],edgeProperties[i][j])
            }
            return
        }
        else{
        nextVertex = vertexAdder(g, vertexNames[i],vertexTypes[i],vertexProperties[i])
        edgeAdder(g,lastVertex,nextVertex,edgeNames[i],edgeProperties[i])
        lastVertex = nextVertex
        }
    }

    
}




start = System.currentTimeMillis() 
last = start
def config = [ 'cache_type':'none' ] 

graph = TitanFactory.open('../../scratch/testing2')
//graph = TitanFactory.open('../../scratch/gha')
graph.createKeyIndex('name',Vertex.class)
BatchGraph bgraph = new BatchGraph(graph, BatchGraph.IdType.OBJECT, 10000)


folder = '../../scratch/githubarchivegz'
baseDir = new File(folder)
fileList = baseDir.listFiles()


if (debug) {
    testFile = '../../scratch/githubarchivegz/2012-04-17-8.json.gz'
    fileList = [testFile]
}


/**
 * could rewrite this so it streams the input but this works for now
 * also loading from temporary json file facilitates debugging
 */

for (file in fileList){
    fileName = file.toString()
    println fileName

    command = 'ruby1.9 FixGitHubArchiveDelimiters.rb ' + fileName + ' ../../scratch/temp2.json'
    process = command.execute()
    process.waitFor()
    myFile = new File('../../scratch/temp2.json').eachLine {line ->loader(bgraph, line)}
}
graph.shutdown()



now = System.currentTimeMillis()  
println 'Total elapsed time in s'
println ((now - start)/1000.0)
