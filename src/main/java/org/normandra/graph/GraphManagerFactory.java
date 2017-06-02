package org.normandra.graph;

import org.normandra.EntityManagerFactory;
import org.normandra.NormandraException;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.GraphMeta;
import org.normandra.meta.QueryMeta;

/**
 * graph manager factory, a core constructor
 */
public class GraphManagerFactory extends EntityManagerFactory {
    private final GraphDatabase database;

    private final GraphMeta graphMeta;

    private boolean configured = false;

    public GraphManagerFactory(final GraphDatabase db, final GraphMeta meta) {
        super(db, meta);
        this.database = db;
        this.graphMeta = meta;
    }

    @Override
    public GraphManager create() throws NormandraException {
        this.ensureDatabase();
        return new GraphManager(this.database.createGraph(), this.graphMeta);
    }

    synchronized private void ensureDatabase() throws NormandraException {
        if (this.configured) {
            return;
        }

        try {
            // refresh schema
            this.database.refresh();
            // register queries
            for (final EntityMeta meta : this.graphMeta) {
                for (final QueryMeta query : meta.getQueries()) {
                    this.database.registerQuery(meta, query.getName(), query.getQuery());
                }
            }
            this.configured = true;
        } catch (final Exception e) {
            throw new NormandraException("Unable to refresh database.", e);
        }
    }
}
