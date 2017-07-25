normandra
=========

Simple client for NoSQL repositories using JPA annotations.  Currently targeting support for Cassandra, OrientDB, and Neo4J.

Download library via current release https://github.com/trajar/normandra/files/1173966/normandra-1.13-all.zip.

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