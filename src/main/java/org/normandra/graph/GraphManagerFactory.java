package org.normandra.graph;

import org.normandra.DatabaseConstruction;
import org.normandra.EntityManagerFactory;
import org.normandra.NormandraException;
import org.normandra.meta.GraphMeta;

/**
 * graph manager factory, a core constructor
 */
public class GraphManagerFactory extends EntityManagerFactory {
    private final GraphDatabase database;

    private final GraphMeta graphMeta;

    public GraphManagerFactory(final GraphDatabase db, final GraphMeta meta, final DatabaseConstruction constructionMode) {
        super(db, meta, constructionMode);
        this.database = db;
        this.graphMeta = meta;
    }

    @Override
    public GraphManager create() throws NormandraException {
        this.ensureDatabase();
        return new GraphManager(this.database.createGraph(), this.graphMeta);
    }
}
