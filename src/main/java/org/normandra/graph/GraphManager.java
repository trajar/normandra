package org.normandra.graph;

import org.normandra.EntityManager;
import org.normandra.NormandraException;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.GraphMeta;

import java.util.*;

/**
 * a graph manager api
 *
 * @date 5/30/17.
 */
public class GraphManager extends EntityManager {
    private final Graph graph;

    private final GraphMeta meta;

    public GraphManager(final Graph db, final GraphMeta meta) {
        super(db, meta);
        this.graph = db;
        this.meta = meta;
    }

    @Override
    public Graph getSession() {
        return this.graph;
    }

    public GraphMeta getMeta() {
        return this.meta;
    }

    /**
     * Add entity as node to graph database.
     *
     * @param entity registered entity
     * @param <T>
     * @return Node instance if created, else null.
     * @throws NormandraException
     */
    public <T> Node<T> addNode(final T entity) throws NormandraException {
        if (null == entity) {
            return null;
        }

        final EntityMeta meta = this.meta.getNodeMeta(entity.getClass());
        if (null == meta) {
            throw new IllegalArgumentException("Entity [" + entity.getClass() + "] is not node type.");
        }

        return this.graph.addNode(meta, entity);
    }

    /**
     * Query node by entity id.
     *
     * @param clazz registered entity
     * @param key   entity key
     * @param <T>
     * @return Node instance if found, else null.
     * @throws NormandraException
     */
    public <T> Node<T> getNode(final Class<T> clazz, final Object key) throws NormandraException {
        if (null == clazz || null == key) {
            return null;
        }

        final EntityMeta meta = this.meta.getNodeMeta(clazz);
        if (null == meta) {
            throw new IllegalArgumentException("Entity [" + clazz + "] is not node type.");
        }

        return this.graph.getNode(meta, key);
    }

    /**
     * Query nodes by set of entity ids.
     *
     * @param clazz registered entity
     * @param keys  set of entity keys
     * @param <T>
     * @return Collection of nodes matching one or more keys.
     * @throws NormandraException
     */
    public <T> Collection<Node> getNodes(final Class<T> clazz, final Iterable<?> keys) throws NormandraException {
        if (null == clazz || null == keys) {
            return Collections.emptyList();
        }

        final EntityMeta meta = this.meta.getNodeMeta(clazz);
        if (null == meta) {
            throw new IllegalArgumentException("Entity [" + clazz + "] is not node type.");
        }

        final Set<Object> filteredKeys = new HashSet<>();
        for (final Object key : keys) {
            if (key != null) {
                filteredKeys.add(key);
            }
        }
        if (filteredKeys.isEmpty()) {
            return Collections.emptyList();
        }

        return this.graph.getNodes(meta, filteredKeys);
    }

    public <T> Collection<Node> getNodes(final Class<T> clazz, final Object... keys) throws NormandraException {
        return getNodes(clazz, Arrays.asList(keys));
    }

    /**
     * Query edge by entity id.
     *
     * @param clazz
     * @param key
     * @param <T>
     * @return
     * @throws NormandraException
     */
    public <T> Edge<T> getEdge(final Class<T> clazz, final Object key) throws NormandraException {
        if (null == clazz || null == key) {
            return null;
        }

        final EntityMeta meta = this.meta.getEdgeMeta(clazz);
        if (null == meta) {
            throw new IllegalArgumentException("Entity [" + clazz + "] is not edge type.");
        }

        return this.graph.getEdge(meta, key);
    }

    /**
     * Query graph nodes by parameters.
     *
     * @param clazz
     * @param query
     * @param parameters
     * @return
     * @throws NormandraException
     */
    public <T> NodeQuery<T> queryNodes(final Class<T> clazz, final String query, final Map<String, Object> parameters) throws NormandraException {
        if (null == clazz || null == query) {
            return null;
        }

        final EntityMeta meta = this.meta.getNodeMeta(clazz);
        if (null == meta) {
            throw new IllegalArgumentException("Entity [" + clazz + "] is not node type.");
        }

        return this.graph.queryNodes(meta, query, parameters);
    }

    /**
     * Query graph edges by parameters.
     *
     * @param clazz
     * @param query
     * @param parameters
     * @return
     * @throws NormandraException
     */
    public <T> EdgeQuery<T> queryEdges(final Class<T> clazz, final String query, final Map<String, Object> parameters) throws NormandraException {
        if (null == clazz || null == query) {
            return null;
        }

        final EntityMeta meta = this.meta.getEdgeMeta(clazz);
        if (null == meta) {
            throw new IllegalArgumentException("Entity [" + clazz + "] is not edge type.");
        }

        return this.graph.queryEdges(meta, query, parameters);
    }
}
