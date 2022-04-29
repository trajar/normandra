package org.normandra.meta;

import org.normandra.data.BasicColumnAccessorFactory;
import org.normandra.data.ColumnAccessorFactory;

import java.util.HashSet;
import java.util.Set;

public class DatabaseMetaBuilder {
    private final Set<Class> classes = new HashSet<>();

    private ColumnAccessorFactory columnAccessorFactory = new BasicColumnAccessorFactory();

    public DatabaseMetaBuilder withColumnFactory(final ColumnAccessorFactory factory) {
        this.columnAccessorFactory = factory;
        return this;
    }

    public DatabaseMetaBuilder withClass(final Class clazz) {
        if (null == clazz) {
            throw new IllegalArgumentException();
        }
        this.classes.add(clazz);
        return this;
    }

    public DatabaseMetaBuilder withClasses(final Class... clazzes) {
        if (null == clazzes) {
            throw new IllegalArgumentException();
        }
        for (final Class<?> clazz : clazzes) {
            this.withClass(clazz);
        }
        return this;
    }

    public DatabaseMetaBuilder withClasses(final Iterable<Class> c) {
        if (null == c) {
            throw new IllegalArgumentException();
        }
        for (final Class<?> clazz : c) {
            this.withClass(clazz);
        }
        return this;
    }

    public GraphMetaBuilder asGraph() {
        final GraphMetaBuilder graph = new GraphMetaBuilder();
        return graph
                .withColumnFactory(this.columnAccessorFactory)
                .withNodeClasses(this.classes);
    }

    public DatabaseMeta create() {
        final AnnotationParser parser = new AnnotationParser(this.columnAccessorFactory, this.classes);
        return new DatabaseMeta(parser.read());
    }
}
