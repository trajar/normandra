package org.normandra.graph;

import org.normandra.Database;
import org.normandra.NormandraException;
import org.normandra.meta.GraphMeta;

/**
 * a graph database
 *
 * @date 5/30/17.
 */
public interface GraphDatabase extends Database {
    Graph createGraph();
    void refresh() throws NormandraException;
}
