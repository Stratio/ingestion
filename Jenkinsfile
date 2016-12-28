@Library('libpipelines@master') _

hose {
    MAIL = 'ingestion'    
    MODULE = 'ingestion'
    REPOSITORY = 'github.com/ingestion'
    DEVTIMEOUT = 45
    RELEASETIMEOUT = 30
    MAXITRETRIES = 2
    
    PKGMODULES = ['stratio-ingestion-dist']
    PKGMODULESNAMES = ['stratio-ingestion']
    DEBARCH = 'all'
    RPMARCH = 'noarch'
    
    ITSERVICES = [
        ['ZOOKEEPER': [
            'image': 'stratio/zookeeper:3.4.6'],
            'sleep': 30,
            'healthcheck': 2181
            ],
        ['MONGODB':[
            'image': 'stratio/mongo:3.0.4',
            'sleep': 30,
            'healthcheck': 27017]],
        ['ELASTICSEARCH':[
            'image': 'stratio/elasticsearch:1.7.1',
            'sleep': 30,
            'healthcheck': 9300,
            'env': ['CLUSTER_NAME=%%JUID']]],
        ['CASSANDRA':[
            'image':'stratio/cassandra-lucene-index:2.2.5.3',
            'sleep': 30,
            'healthcheck': 9042,
            'env':['MAX_HEAP=256M']]],
        ['KAFKA':[
            'image': 'stratio/kafka:0.8.2.1',
            'sleep': 30,
            'healthcheck': 9092,
            'env': ['ZOOKEEPER_HOSTS=%%ZOOKEEPER:2181']]]
    ]

    ITPARAMETERS = """
        | -Dzookeeper.hosts.0=%%ZOOKEEPER:2181
        | -Dkafka.hosts.0=%%KAFKA:9092
        | -Dmongo.hosts.0=%%MONGODB:27017
        | -Dcassandra.hosts.0=%%CASSANDRA:9042
        | -Delasticsearch.hosts.0=%%ELASTICSEARCH:9300
        | -Delasticsearch.clusterName=%%JUID
        | """

    DEV = { config ->
        doCompile(config)

        parallel(UT: {
            doUT(config)
        }, IT: {
            doIT(config)
        }, failFast: config.FAILFAST)

        doPackage(config)
        doDeploy(config)

        parallel(DOC: {
           doDoc(config)
         },QC: {
            doStaticAnalysis(config)    
        }, DOCKER: {
            doDocker(config)
        }, failFast: config.FAILFAST)
        
     }
}
