normandra
=========

Simple client for NoSQL repositories using JPA annotations.  Currently targeting support for Cassandra, OrientDB, and Neo4J.

Download library via 

```
meta = new GraphMetaBuilder()
            .withNodeClasses(nodes)
            .withEdgeClasses(edges)
            .create();
database = OrientGraphDatabase.create(
            pathOrUrl, 
            new MemoryCache.Factory(MapFactory.withConcurrency()),
            DatabaseConstruction.CREATE, builder);
graphManager = new GraphManagerFactory(database, meta);            
```