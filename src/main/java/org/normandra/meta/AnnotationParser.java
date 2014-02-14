package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.BasicFieldColumnAccessor;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.JoinColumnAccessor;
import org.normandra.data.ListColumnAccessor;
import org.normandra.data.NestedFieldColumnAccessor;
import org.normandra.data.SetColumnAccessor;
import org.normandra.util.CaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * a class used to parse jpa annotations
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class AnnotationParser
{
    private static final Logger logger = LoggerFactory.getLogger(AnnotationParser.class);

    private final List<Class> classes;

    private final Map<Class, EntityMeta> entities = new HashMap<>();


    public AnnotationParser(final Class clazz, final Class... list)
    {
        if (null == clazz)
        {
            throw new NullArgumentException("class");
        }
        this.classes = new ArrayList<>();
        this.classes.add(clazz);
        if (list != null && list.length > 0)
        {
            for (final Class<?> item : list)
            {
                if (item != null)
                {
                    this.classes.add(item);
                }
            }
        }
    }


    public AnnotationParser(final Collection<Class> c)
    {
        if (null == c)
        {
            throw new NullArgumentException("classes");
        }
        this.classes = new ArrayList<>(c);
    }


    public Set<EntityMeta> read()
    {
        final Set<EntityMeta> list = new TreeSet<>();
        for (final Class<?> clazz : this.classes)
        {
            final EntityMeta<?> meta = this.readEntity(clazz);
            if (meta != null)
            {
                list.add(meta);
            }
        }
        return Collections.unmodifiableSet(list);
    }


    private <T> EntityMeta<T> readEntity(final Class<T> entityClass)
    {
        final EntityMeta<T> existing = this.entities.get(entityClass);
        if (existing != null)
        {
            return existing;
        }

        boolean entity = false;
        for (final Class<?> clazz : this.getHierarchy(entityClass))
        {
            if (clazz.getAnnotation(Entity.class) != null)
            {
                entity = true;
                break;
            }
        }
        if (!entity)
        {
            return null;
        }

        final String name = this.getEntity(entityClass);
        final String table = this.getTable(entityClass);
        final Collection<ColumnMeta> columns = this.getColumns(entityClass);
        final EntityMeta<T> meta = new EntityMeta<>(name, table, entityClass, columns);
        this.entities.put(entityClass, meta);
        return meta;
    }


    public boolean isEntity(final Class<?> entityClass)
    {
        return this.readEntity(entityClass) != null;
    }


    protected String getEntity(final Class<?> entityClass)
    {
        final Entity entity = entityClass.getAnnotation(Entity.class);
        if (null == entity)
        {
            return null;
        }
        final String name = entity.name();
        if (name != null && !name.isEmpty())
        {
            return name;
        }
        return entityClass.getSimpleName();
    }


    protected String getTable(final Class<?> entityClass)
    {
        for (final Class<?> clazz : this.getHierarchy(entityClass))
        {
            final Table table = clazz.getAnnotation(Table.class);
            final String name = table != null ? table.name() : null;
            if (name != null && !name.isEmpty())
            {
                return table.name();
            }
        }
        return CaseUtils.camelToSnakeCase(entityClass.getSimpleName());
    }


    public Map<Field, GeneratedValue> getGenerators(final Class<?> entityClass)
    {
        final Map<Field, GeneratedValue> map = new HashMap<>();
        for (final Field field : this.getFields(entityClass))
        {
            final GeneratedValue annotation = field.getAnnotation(GeneratedValue.class);
            if (annotation != null)
            {
                map.put(field, annotation);
            }
        }
        return Collections.unmodifiableMap(map);
    }


    public <T extends Annotation> List<T> getAnnotations(final Class<?> entityClass, final Class<T> clazz)
    {
        final List<T> list = new ArrayList<>();
        for (final Field field : this.getFields(entityClass))
        {
            final T annotation = field.getAnnotation(clazz);
            if (annotation != null)
            {
                list.add(annotation);
            }
        }
        for (final Class<?> type : this.getHierarchy(entityClass))
        {
            final T annotation = type.getAnnotation(clazz);
            if (annotation != null)
            {
                list.add(annotation);
            }
        }
        return Collections.unmodifiableList(list);
    }


    protected List<ColumnMeta> getColumns(final Class<?> entityClass)
    {
        final List<Field> fields = this.getFields(entityClass);
        if (null == fields || fields.isEmpty())
        {
            return Collections.emptyList();
        }

        final List<ColumnMeta> columns = new ArrayList<>();
        for (final Field field : fields)
        {
            if (this.configureField(entityClass, field, columns))
            {
                logger.debug("Configured metadata for [" + field + "] in [" + entityClass + "].");
            }
            else
            {
                logger.warn("Unable to configure metadata for [" + field + "] in [" + entityClass + "].");
            }
        }
        if (columns.isEmpty())
        {
            return Collections.emptyList();
        }

        final ColumnMeta meta = this.getDiscriminator(entityClass);
        if (meta != null)
        {
            columns.add(meta);
        }

        return Collections.unmodifiableList(columns);
    }


    protected List<Field> getFields(final Class<?> entityClass)
    {
        final List<Field> list = new ArrayList<>();
        for (final Class<?> clazz : this.getHierarchyReverse(entityClass))
        {
            final Field[] fields = clazz.getDeclaredFields();
            if (fields != null && fields.length > 0)
            {
                for (final Field field : fields)
                {
                    if (!Modifier.isTransient(field.getModifiers()))
                    {
                        if (field.isAnnotationPresent(Column.class) ||
                                field.isAnnotationPresent(EmbeddedId.class) ||
                                field.isAnnotationPresent(Id.class) ||
                                field.isAnnotationPresent(ElementCollection.class) ||
                                field.isAnnotationPresent(JoinColumn.class) ||
                                field.isAnnotationPresent(OneToMany.class) ||
                                field.isAnnotationPresent(ManyToOne.class) ||
                                field.isAnnotationPresent(ManyToMany.class) ||
                                field.isAnnotationPresent(OneToOne.class))
                        {
                            list.add(field);
                        }
                    }
                }
            }
        }
        return list;
    }


    protected boolean configureField(final Class<?> entityClass, final Field field, final Collection<ColumnMeta> columns)
    {
        // basic column info
        final Column column = field.getAnnotation(Column.class);
        final JoinColumn join = field.getAnnotation(JoinColumn.class);
        final String property = field.getName();
        final Class<?> type = field.getType();
        String name = CaseUtils.camelToSnakeCase(field.getName());
        if (column != null && !column.name().trim().isEmpty())
        {
            name = column.name();
        }
        else if (join != null && !join.name().trim().isEmpty())
        {
            name = join.name();
        }

        // setup guid
        final EmbeddedId embeddedId = field.getAnnotation(EmbeddedId.class);
        final Id id = field.getAnnotation(Id.class);
        if (id != null)
        {
            final ColumnAccessor accessor = new BasicFieldColumnAccessor(field, type);
            columns.add(new ColumnMeta<>(name, property, accessor, type, true));
            return true;
        }
        else if (embeddedId != null)
        {
            final Embeddable embeddable = type.getAnnotation(Embeddable.class);
            if (null == embeddable)
            {
                throw new IllegalStateException("Class [" + type + "] does not have Embeddable annotation.");
            }
            for (final Field embeddedColumn : new AnnotationParser(type).getFields(entityClass))
            {
                final Class<?> embeddedClass = embeddedColumn.getType();
                final ColumnAccessor accessor = new NestedFieldColumnAccessor(field, new BasicFieldColumnAccessor(embeddedColumn, embeddedClass));
                columns.add(new ColumnMeta<>(name, property, accessor, type, true));
            }
        }

        // regular column
        if (field.isAnnotationPresent(ElementCollection.class))
        {
            return this.configureElementCollection(field, name, property, columns);
        }

        // associations and join columns
        if (field.isAnnotationPresent(OneToOne.class))
        {
            return this.configureOneToOne(field, name, property, columns);
        }
        else if (field.isAnnotationPresent(ManyToOne.class))
        {
            return this.configureManyToOne(field, name, property, columns);
        }

        // regular column
        if (column != null)
        {
            final ColumnAccessor accessor = new BasicFieldColumnAccessor(field, type);
            columns.add(new ColumnMeta<>(name, property, accessor, type, false));
            return true;
        }

        // not jpa column
        return false;
    }


    private boolean configureOneToOne(final Field field, final String name, final String property, final Collection<ColumnMeta> columns)
    {
        final OneToOne oneToOne = field.getAnnotation(OneToOne.class);
        final boolean lazy = FetchType.LAZY.equals(oneToOne.fetch());
        final String mappedBy = oneToOne.mappedBy().trim();
        if (mappedBy.isEmpty())
        {
            // create table column for this relationship
            final Class<?> type = field.getType();
            final EntityMeta<?> entity = this.readEntity(type);
            if (null == entity)
            {
                throw new IllegalStateException("Type [" + type + "] is not a registered entity.");
            }
            final Class<?> keyType = entity.getPartition().getType();
            final ColumnAccessor accessor = new JoinColumnAccessor(field, entity, keyType, lazy);
            columns.add(new JoinColumnMeta<>(name, property, accessor, keyType, entity));
            return true;
        }
        else
        {
            // this table does not own this column
            return false;
        }
    }


    private boolean configureManyToOne(final Field field, final String name, final String property, final Collection<ColumnMeta> columns)
    {
        final ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
        final boolean lazy = FetchType.LAZY.equals(manyToOne.fetch());
        final Class<?> type = field.getType();
        final EntityMeta<?> entity = this.readEntity(type);
        if (null == entity)
        {
            throw new IllegalStateException("Type [" + type + "] is not a registered entity.");
        }
        final Class<?> keyType = entity.getPartition().getType();
        final ColumnAccessor accessor = new JoinColumnAccessor(field, entity, keyType, lazy);
        columns.add(new JoinColumnMeta<>(name, property, accessor, keyType, entity));
        return true;
    }


    protected boolean configureElementCollection(final Field field, final String name, final String property, final Collection<ColumnMeta> columns)
    {
        final Class<?> type = field.getType();
        final ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
        final Type[] types = parameterizedType.getActualTypeArguments();
        final Class<?> parameterizedClass = types != null && types.length > 0 ? (Class<?>) types[0] : null;
        if (null == parameterizedClass)
        {
            return false;
        }

        if (this.isEntity(parameterizedClass))
        {
            return false;
        }

        final ColumnAccessor accessor;
        if (type.isInstance(Set.class))
        {
            accessor = new SetColumnAccessor(field, parameterizedClass);
        }
        else
        {
            accessor = new ListColumnAccessor(field, parameterizedClass);
        }
        columns.add(new CollectionMeta(name, property, accessor, type, parameterizedClass));
        return true;
    }


    protected ColumnMeta getDiscriminator(final Class<?> entityClass)
    {
        for (final Class<?> clazz : this.getHierarchy(entityClass))
        {
            if (!Modifier.isAbstract(clazz.getModifiers()))
            {
                final DiscriminatorColumn column = clazz.getAnnotation(DiscriminatorColumn.class);
                if (column != null)
                {
                    final String name = column.name();
                    final String property = "DiscriminatorColumn";
                    final DiscriminatorType type = column.discriminatorType();
                    final DiscriminatorValue value = clazz.getAnnotation(DiscriminatorValue.class);
                    String discriminator = value != null ? value.value() : "";
                    if (null == discriminator || discriminator.isEmpty())
                    {
                        discriminator = CaseUtils.camelToSnakeCase(entityClass.getSimpleName());
                    }
                    if (type != null)
                    {
                        switch (type)
                        {
                            case CHAR:
                                return new DiscriminatorMeta<>(name, property, discriminator.charAt(0), Character.class);
                            case INTEGER:
                                return new DiscriminatorMeta<>(name, property, Integer.parseInt(discriminator), Integer.class);
                            case STRING:
                            default:
                                return new DiscriminatorMeta<>(name, property, discriminator, String.class);
                        }
                    }
                }
            }
        }
        return null;
    }


    private List<Class<?>> getHierarchy(final Class<?> entityClass)
    {
        if (null == entityClass)
        {
            return Collections.emptyList();
        }
        final List<Class<?>> list = new ArrayList<>();
        list.add(entityClass);
        Class<?> parent = entityClass.getSuperclass();
        while (parent != null && !Object.class.equals(parent))
        {
            list.add(parent);
            parent = parent.getSuperclass();
        }
        return Collections.unmodifiableList(list);
    }


    private List<Class<?>> getHierarchyReverse(final Class<?> entityClass)
    {
        if (null == entityClass)
        {
            return Collections.emptyList();
        }
        final List<Class<?>> list = new ArrayList<>(this.getHierarchy(entityClass));
        Collections.reverse(list);
        return Collections.unmodifiableList(list);
    }
}
