package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.BasicFieldColumnAccessor;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.ListColumnAccessor;
import org.normandra.data.NestedFieldColumnAccessor;
import org.normandra.data.ReadOnlyColumnAccessor;
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
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * a class used to parse jpa annotations
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class AnnotationParser
{
    private static final Logger logger = LoggerFactory.getLogger(AnnotationParser.class);

    private final Class<?> entityClass;


    public AnnotationParser(final Class<?> clazz)
    {
        if (null == clazz)
        {
            throw new NullArgumentException("class");
        }
        this.entityClass = clazz;
    }


    public EntityMeta readEntity()
    {
        if (!this.isEntity())
        {
            return null;
        }

        final String name = this.getEntity();
        final String table = this.getTable();
        final Collection<ColumnMeta> columns = this.getColumns();
        return new EntityMeta(name, table, this.entityClass, columns);
    }


    public boolean isEntity()
    {
        return this.entityClass.isAnnotationPresent(Entity.class);
    }


    public String getEntity()
    {
        final Entity entity = this.entityClass.getAnnotation(Entity.class);
        if (null == entity)
        {
            return null;
        }
        final String name = entity.name();
        if (name != null && !name.isEmpty())
        {
            return name;
        }
        return this.entityClass.getSimpleName();
    }


    public String getTable()
    {
        for (final Class<?> clazz : this.getHierarchy())
        {
            final Table table = clazz.getAnnotation(Table.class);
            final String name = table != null ? table.name() : null;
            if (name != null && !name.isEmpty())
            {
                return table.name();
            }
        }
        return CaseUtils.camelToSnakeCase(this.entityClass.getSimpleName());
    }


    public List<ColumnMeta> getColumns()
    {
        final List<Field> fields = this.getFields();
        if (null == fields || fields.isEmpty())
        {
            return Collections.emptyList();
        }

        final List<ColumnMeta> columns = new ArrayList<>();
        for (final Field field : fields)
        {
            if (this.configureField(field, columns))
            {
                logger.debug("Configured metadata for [" + field + "] in [" + this.entityClass + "].");
            }
            else
            {
                logger.warn("Unable to configure metadata for [" + field + "] in [" + this.entityClass + "].");
            }
        }
        if (columns.isEmpty())
        {
            return Collections.emptyList();
        }

        final ColumnMeta meta = this.getDiscriminator();
        if (meta != null)
        {
            columns.add(meta);
        }

        return Collections.unmodifiableList(columns);
    }


    private List<Field> getFields()
    {
        final List<Field> list = new ArrayList<>();
        for (final Class<?> clazz : this.getHierarchyReverse())
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


    private boolean configureField(final Field field, final Collection<ColumnMeta> columns)
    {
        // basic column info
        final Column column = field.getAnnotation(Column.class);
        final Class<?> type = field.getType();
        String name = column != null ? column.name() : null;
        if (null == name || name.isEmpty())
        {
            name = CaseUtils.camelToSnakeCase(field.getName());
        }

        // setup guid
        final EmbeddedId embeddedId = field.getAnnotation(EmbeddedId.class);
        final Id id = field.getAnnotation(Id.class);
        if (id != null)
        {
            final ColumnAccessor accessor = new BasicFieldColumnAccessor(field, type);
            columns.add(new ColumnMeta(name, accessor, type, true));
            return true;
        }
        else if (embeddedId != null)
        {
            final Embeddable embeddable = type.getAnnotation(Embeddable.class);
            if (null == embeddable)
            {
                throw new IllegalStateException("Class [" + type + "] does not have Embeddable annotation.");
            }
            for (final Field embeddedColumn : new AnnotationParser(type).getFields())
            {
                final Class<?> embeddedClass = embeddedColumn.getType();
                final ColumnAccessor accessor = new NestedFieldColumnAccessor(field, new BasicFieldColumnAccessor(embeddedColumn, embeddedClass));
                columns.add(new ColumnMeta(name, accessor, type, true));
            }
        }

        // regular column
        if (null == column)
        {
            return false;
        }
        if (Collection.class.equals(type) || field.isAnnotationPresent(ElementCollection.class))
        {
            final ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
            final Type[] types = parameterizedType.getActualTypeArguments();
            final Class<?> parameterizedClass = types != null && types.length > 0 ? (Class<?>) types[0] : null;
            if (parameterizedClass != null)
            {
                final AnnotationParser parameterizedParser = new AnnotationParser(parameterizedClass);
                if (!parameterizedParser.isEntity())
                {
                    // generic element collection
                    final ColumnAccessor accessor;
                    if (type.isInstance(Set.class))
                    {
                        accessor = new SetColumnAccessor(field, parameterizedClass);
                    }
                    else
                    {
                        accessor = new ListColumnAccessor(field, parameterizedClass);
                    }
                    columns.add(new ColumnMeta(name, accessor, type, false));
                }
            }
        }
        else
        {
            final ColumnAccessor accessor = new BasicFieldColumnAccessor(field, type);
            columns.add(new ColumnMeta(name, accessor, type, false));
        }
        return true;
    }


    private ColumnMeta getDiscriminator()
    {
        for (final Class<?> clazz : this.getHierarchy())
        {
            if (!Modifier.isAbstract(clazz.getModifiers()))
            {
                final DiscriminatorColumn column = clazz.getAnnotation(DiscriminatorColumn.class);
                if (column != null)
                {
                    final String name = column.name();
                    final DiscriminatorType type = column.discriminatorType();
                    final DiscriminatorValue value = clazz.getAnnotation(DiscriminatorValue.class);
                    String discriminator = value != null ? value.value() : "";
                    if (null == discriminator || discriminator.isEmpty())
                    {
                        discriminator = CaseUtils.camelToSnakeCase(this.entityClass.getSimpleName());
                    }
                    final ColumnAccessor accessor = new ReadOnlyColumnAccessor(discriminator);
                    if (type != null)
                    {
                        switch (type)
                        {
                            case CHAR:
                                return new ColumnMeta(name, accessor, Character.class, false);
                            case INTEGER:
                                return new ColumnMeta(name, accessor, Integer.class, false);
                            case STRING:
                            default:
                                return new ColumnMeta(name, accessor, String.class, false);
                        }
                    }
                }
            }
        }
        return null;
    }


    private List<Class<?>> getHierarchy()
    {
        final List<Class<?>> list = new ArrayList<>();
        list.add(this.entityClass);
        Class<?> parent = this.entityClass.getSuperclass();
        while (parent != null && !Object.class.equals(parent))
        {
            list.add(parent);
            parent = parent.getSuperclass();
        }
        return Collections.unmodifiableList(list);
    }


    private List<Class<?>> getHierarchyReverse()
    {
        final List<Class<?>> list = new ArrayList<>(this.getHierarchy());
        Collections.reverse(list);
        return Collections.unmodifiableList(list);
    }
}
