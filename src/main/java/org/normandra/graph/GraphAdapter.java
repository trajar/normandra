package org.normandra.graph;

import org.normandra.AbstractTransactional;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.Transaction;
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
            try (final Transaction tx = this.beginTransaction()) {
                final Node node = this.getNode(meta, id);
                if (node != null) {
                    node.refresh();
                    node.updateEntity(element);
                } else {
                    addNode(meta, element);
                }
                tx.success();
            } catch (final Exception e) {
                throw new NormandraException("Unable to update node.", e);
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
            try (final Transaction tx = this.beginTransaction()) {
                final Node node = this.getNode(meta, id);
                if (node != null) {
                    node.refresh();
                    node.delete();
                    tx.success();
                }
            } catch (final Exception e) {
                throw new NormandraException("Unable to delete node.", e);
            }
        } else {
            throw new IllegalStateException("Cannot delete graph entity without id.");
        }
    }
}
