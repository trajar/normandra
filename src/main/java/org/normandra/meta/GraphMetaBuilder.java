package org.normandra.meta;

import org.normandra.data.BasicColumnAccessorFactory;
import org.normandra.data.ColumnAccessorFactory;
import org.normandra.property.EmptyPropertyFilter;
import org.normandra.property.PropertyFilter;

import java.util.*;

public class GraphMetaBuilder {
    private final Set<Class> nodeClasses = new HashSet<>();

    private final Set<Class> edgeClasses = new HashSet<>();

    private final Set<Class> entityClasses = new HashSet<>();

    private final Map<Class, PropertyFilter> propertyFilters = new HashMap<>();

    private PropertyFilter globalPropertyFilter = EmptyPropertyFilter.getInstance();

    private ColumnAccessorFactory columnAccessorFactory = new BasicColumnAccessorFactory();

    public GraphMetaBuilder withColumnFactory(final ColumnAccessorFactory factory) {
        this.columnAccessorFactory = factory;
        return this;
    }

    public GraphMetaBuilder withNodeClass(final Class<?> clazz) {
        if (null == clazz) {
            throw new IllegalArgumentException();
        }
        this.nodeClasses.add(clazz);
        return this;
    }

    public GraphMetaBuilder withNodeClasses(final Class... clazzes) {
        if (null == clazzes) {
            throw new IllegalArgumentException();
        }
        for (final Class<?> clazz : clazzes) {
            if (clazz != null) {
                this.withNodeClass(clazz);
            }
        }
        return this;
    }

    public GraphMetaBuilder withNodeClasses(final Iterable<Class> c) {
        if (null == c) {
            throw new IllegalArgumentException();
        }
        for (final Class<?> clazz : c) {
            if (clazz != null) {
                this.withNodeClass(clazz);
            }
        }
        return this;
    }

    public GraphMetaBuilder withEdgeClass(final Class<?> clazz) {
        if (null == clazz) {
            throw new IllegalArgumentException();
        }
        this.edgeClasses.add(clazz);
        return this;
    }

    public GraphMetaBuilder withEdgeClasses(final Class... clazzes) {
        if (null == clazzes) {
            throw new IllegalArgumentException();
        }
        for (final Class<?> clazz : clazzes) {
            if (clazz != null) {
                this.withEdgeClass(clazz);
            }
        }
        return this;
    }

    public GraphMetaBuilder withEdgeClasses(final Iterable<Class> c) {
        if (null == c) {
            throw new IllegalArgumentException();
        }
        for (final Class<?> clazz : c) {
            if (clazz != null) {
                this.withEdgeClass(clazz);
            }
        }
        return this;
    }

    public GraphMetaBuilder withEntityClass(final Class<?> clazz) {
        if (null == clazz) {
            throw new IllegalArgumentException();
        }
        this.entityClasses.add(clazz);
        return this;
    }

    public GraphMetaBuilder withEntityClasses(final Class... clazzes) {
        if (null == clazzes) {
            throw new IllegalArgumentException();
        }
        for (final Class<?> clazz : clazzes) {
            if (clazz != null) {
                this.withEntityClass(clazz);
            }
        }
        return this;
    }

    public GraphMetaBuilder withEntityClasses(final Iterable<Class> c) {
        if (null == c) {
            throw new IllegalArgumentException();
        }
        for (final Class<?> clazz : c) {
            if (clazz != null) {
                this.withEntityClass(clazz);
            }
        }
        return this;
    }

    public GraphMetaBuilder withPropertyFilterFor(final Class<?> clazz, final PropertyFilter filter) {
        if (null == clazz) {
            throw new IllegalArgumentException();
        }
        if (null == filter) {
            this.propertyFilters.remove(clazz);
        } else {
            this.propertyFilters.put(clazz, filter);
        }
        return this;
    }

    public GraphMetaBuilder withPropertyFilter(final PropertyFilter filter) {
        if (null == filter) {
            this.globalPropertyFilter = EmptyPropertyFilter.getInstance();
        } else {
            this.globalPropertyFilter = filter;
        }
        return this;
    }

    public GraphMeta create() {
        // read all entities
        final Set<Class> allClasses = new HashSet<>();
        allClasses.addAll(nodeClasses);
        allClasses.addAll(edgeClasses);
        allClasses.addAll(entityClasses);
        final AnnotationParser parser = new AnnotationParser(columnAccessorFactory, allClasses);
        final Set<EntityMeta> allMetas = parser.read();

        // setup database
        final Set<EntityMeta> nodeMetas = new LinkedHashSet<>();
        final Set<EntityMeta> edgeMetas = new LinkedHashSet<>();
        final Set<EntityMeta> genericMetas = new LinkedHashSet<>();
        allMetas.forEach(item -> {
            for (final Class<?> clazz : item.getTypes()) {
                if (nodeClasses.contains(clazz)) {
                    nodeMetas.add(item);
                }
            }
        });
        allMetas.forEach(item -> {
            for (final Class<?> clazz : item.getTypes()) {
                if (edgeClasses.contains(clazz)) {
                    edgeMetas.add(item);
                }
            }
        });
        allMetas.forEach(item -> {
            for (final Class<?> clazz : item.getTypes()) {
                if (entityClasses.contains(clazz)) {
                    genericMetas.add(item);
                }
            }
        });
        final GraphMeta meta = new GraphMeta(nodeMetas, edgeMetas, genericMetas);
        if (this.globalPropertyFilter != null) {
            for (final Class<?> clazz : this.nodeClasses) {
                final EntityMeta entity = meta.getNodeMeta(clazz);
                if (entity != null) {
                    meta.setPropertyFilter(entity, this.globalPropertyFilter);
                }
            }
            for (final Class<?> clazz : this.edgeClasses) {
                final EntityMeta entity = meta.getEdgeMeta(clazz);
                if (entity != null) {
                    meta.setPropertyFilter(entity, this.globalPropertyFilter);
                }
            }
        }
        for (final Map.Entry<Class, PropertyFilter> entry : this.propertyFilters.entrySet()) {
            final Class<?> clazz = entry.getClass();
            EntityMeta entity = meta.getNodeMeta(clazz);
            if (null == entity) {
                entity = meta.getEdgeMeta(clazz);
            }
            if (entity != null) {
                meta.setPropertyFilter(entity, entry.getValue());
            }
        }
        return meta;
    }
}
