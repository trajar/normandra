package org.normandra.config;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.util.CaseUtils;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * a class used to parse jpa annotations
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class AnnotationParser
{
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
        final Map<String, Field> map = new LinkedHashMap<>();
        for (final Class<?> clazz : this.getHierarchyReverse())
        {
            final Field[] fields = clazz.getDeclaredFields();
            if (fields != null && fields.length > 0)
            {
                for (final Field field : fields)
                {
                    if (!Modifier.isTransient(field.getModifiers()))
                    {
                        map.put(field.getName(), field);
                    }
                }
            }
        }

        if (map.isEmpty())
        {
            return Collections.emptyList();
        }

        final Map<String, ColumnMeta> columns = new LinkedHashMap<>();
        for (final Field field : map.values())
        {
            final Id id = field.getAnnotation(Id.class);
            final Column column = field.getAnnotation(Column.class);
            if (id != null || column != null)
            {
                // property name
                String name = column != null ? column.name() : null;
                if (null == name || name.isEmpty())
                {
                    name = CaseUtils.camelToSnakeCase(field.getName());
                }

                // property type
                final Class<?> type = field.getType();

                // build meta-data
                final ColumnMeta meta;
                if (Collection.class.equals(type) || field.isAnnotationPresent(ElementCollection.class))
                {
                    final ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
                    final Class<?> parameterizedClass = (Class<?>) parameterizedType.getActualTypeArguments()[0];
                    meta = new CollectionMeta(name, type, parameterizedClass);
                }
                else
                {
                    meta = new ColumnMeta(name, type, id != null);
                }
                columns.put(name, meta);
            }
        }

        final ColumnMeta meta = this.getDiscriminator();
        if (meta != null)
        {
            columns.put(meta.getName(), meta);
        }

        return new ArrayList<>(columns.values());
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
                    if (type != null)
                    {
                        switch (type)
                        {
                            case CHAR:
                                return new ColumnMeta(name, Character.class, false);
                            case INTEGER:
                                return new ColumnMeta(name, Integer.class, false);
                            case STRING:
                            default:
                                return new ColumnMeta(name, String.class, false);
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
