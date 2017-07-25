normandra
=========

Simple client for NoSQL repositories using JPA annotations.  Currently targeting support for Cassandra, OrientDB, and Neo4J.

Download library via current release https://github.com/trajar/normandra/files/1173966/normandra-1.13-all.zip.

```
builder = new GraphMetaBuilder()
            .withNodeClasses(nodes)
            .withEdgeClasses(edges);
database = OrientGraphDatabase.create(
            pathOrUrl, 
            new MemoryCache.Factory(MapFactory.withConcurrency()),
            DatabaseConstruction.CREATE, builder);
meta = builder.create();
graphManager = new GraphManagerFactory(database, meta);            
```