normandra
=========

Simple client for NoSQL repositories using JPA annotations.  Currently targeting support for Cassandra, OrientDB, and Neo4J.

Download library via current release https://github.com/trajar/normandra/releases/tag/v1.24.

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
