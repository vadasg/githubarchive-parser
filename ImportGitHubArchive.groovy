import groovy.json.JsonSlurper
import com.thinkaurelius.titan.core.*

/** 
 * Reads in gzipped githubarchive json files in a specified directory
 * and loads into Titan graph
 * @author Vadas Gintautas
 */


debug = false
slurper = new JsonSlurper()
count = 0

/**
 * Written as function instead of closure due to recursion
 */
def safePropertyAdder(currentObject, propertyMap){
    for (pair in propertyMap) {
        if (pair.value == null) return
        //if (pair.value.getClass().equals(java.util.HashMap)){
        //    //process maps recursively
        //    safePropertyAdder(currentObject,pair.value)
        //}
        if (pair.key in ['type','name','id','label']){
            currentObject.setProperty('github_' + pair.key,pair.value)
        } else {
            currentObject.setProperty(pair.key,pair.value)
        }
    }
}


def vertexAdder = {g,  name, type, properties ->
    if (name==null) throw new IllegalArgumentException('Name cannot be null')
    if (name==null) throw new IllegalArgumentException('Type cannot be null')

    vertex=g.getVertex(name)
    if (vertex==null) {
        vertex=g.addVertex(name)
        safePropertyAdder(vertex,properties)
    }
    vertex.setProperty('name',name)
    vertex.setProperty('type',type)
    return vertex
}


def edgeAdder = {g,inVertex, outVertex, name, properties->
    if (name==null) throw new IllegalArgumentException('Name cannot be null')
    
    edge = g.addEdge(null,outVertex,inVertex,name)
    safePropertyAdder(edge,properties)
    edge.setProperty('name',name)
    return edge
}


def loader = {g, line -> 
    s = slurper.parseText(line)
    if (s.actor == null) return

    if (debug) {
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
        'GollumEvent':'editedWikiOf',
        'PublicEvent':'madePublic',
        'PullRequestEvent':'pullRequested',
        'PushEvent':'pushed'
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


    if (debug) println vertexNames
    for (i in 0..vertexNames.size()-1) {
        nextVertex = vertexAdder(g, vertexNames[i],vertexTypes[i],vertexProperties[])

        //here we add a vertex for Organizition and corresponding edges
        if ((vertexTypes[i] == 'Repository') 
            && (repoOrg != null)){
            repositoryVertex = vertexAdder(g,repoOrg,'Organization',[:])
            edgeAdder(g,repositoryVertex,nextVertex,'owns',[:])
        }

        edgeAdder(g,lastVertex,nextVertex,edgeNames[i],edgeProperties[i])
        lastVertex = nextVertex
    }

    count = count + 1
    if (count % 1000000 == 0 ) { 
        now = System.currentTimeMillis()
        sec = (now - last)/1000
        println sec.toString() +  ' s for ' + count.toString()
        last = now
    }
    
}


if (debug) {
    testFile = '../../scratch/githubarchivegz/2012-04-17-8.json.gz'
    fileList = [testFile]
}


start = System.currentTimeMillis() 
last = start
def config = [ 'cache_type':'none' ] 

graph = TitanFactory.open('../../scratch/testing')
//graph = TitanFactory.open('../../scratch/gha')
graph.createKeyIndex('name',Vertex.class)
BatchGraph bgraph = new BatchGraph(graph, BatchGraph.IdType.OBJECT, 10000)


folder = '../../scratch/githubarchivegz'
baseDir = new File(folder)
fileList = baseDir.listFiles()




/**
 * could rewrite this so it streams the input but this works for now
 * also loading from temporary json file facilitates debugging
 */

for (file in fileList){
    fileName = file.toString()
    println fileName

    command = 'ruby1.9 FixGitHubArchiveDelimiters.rb ' + fileName + ' ../../scratch/temp.json'
    process = command.execute()
    process.waitFor()
    myFile = new File('../../scratch/temp.json').eachLine {line ->loader(bgraph, line)}
}
graph.shutdown()



now = System.currentTimeMillis()  
println 'Total elapsed time in s'
println (now - start)/1000
