package org.normandra.graph;

import org.normandra.AbstractTransactional;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.GraphMeta;

/**
 * an adapter for graph databases to use as entity model
 */
abstract public class GraphAdapter extends AbstractTransactional implements DatabaseSession, Graph {
    protected final GraphMeta meta;

    protected GraphAdapter(final GraphMeta meta) {
        this.meta = meta;
    }

    @Override
    public void save(EntityMeta meta, Object element) throws NormandraException {
        if (null == meta || null == element) {
            throw new IllegalArgumentException("Meta/element cannot be null.");
        }

        final Object id = meta.getId().fromEntity(element);
        if (id != null) {
            final Node node = this.getNode(meta, id);
            if (node != null) {
                node.updateEntity(element);
            } else {
                addNode(meta, element);
            }
        } else {
            addNode(meta, element);
        }
    }

    @Override
    public void delete(EntityMeta meta, Object element) throws NormandraException {
        if (null == meta || null == element) {
            throw new IllegalArgumentException("Meta/element cannot be null.");
        }

        final Object id = meta.getId().fromEntity(element);
        if (id != null) {
            final Node node = this.getNode(meta, id);
            if (node != null) {
                node.delete();
            }
        } else {
            throw new IllegalStateException("Cannot delete graph entity without id.");
        }
    }
}
