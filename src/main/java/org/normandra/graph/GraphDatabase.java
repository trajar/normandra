package org.normandra.graph;

import org.normandra.Database;
import org.normandra.NormandraException;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.GraphMeta;

/**
 * a graph database
 *
 * @date 5/30/17.
 */
public interface GraphDatabase extends Database {
    GraphMeta getMeta();
    Graph createGraph();
    void refresh() throws NormandraException;
}
