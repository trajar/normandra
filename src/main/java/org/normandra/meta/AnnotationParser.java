package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.BasicColumnAccessor;
import org.normandra.data.BasicIdAccessor;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.CompositeIdAccessor;
import org.normandra.data.ListColumnAccessor;
import org.normandra.data.ManyJoinColumnAccessor;
import org.normandra.data.NestedColumnAccessor;
import org.normandra.data.NullIdAccessor;
import org.normandra.data.ReadOnlyColumnAccessor;
import org.normandra.data.SetColumnAccessor;
import org.normandra.data.SingleJoinColumnAccessor;
import org.normandra.util.CaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorValue;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
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
 * <p>
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
        // read first pass for all entities
        for (final Class<?> clazz : this.classes)
        {
            this.readEntity(clazz);
        }
        // ensure we read second pass
        final TreeSet<EntityMeta> set = new TreeSet<>(this.entities.values());
        for (final EntityMeta entity : set)
        {
            this.readSecondPass(entity);
        }
        // done
        return Collections.unmodifiableSet(set);
    }


    private <T> EntityMeta readEntity(final Class<T> entityClass)
    {
        if (!this.classes.contains(entityClass))
        {
            return null;
        }

        final EntityMeta existing = this.entities.get(entityClass);
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

        // create entity first - to help resolve cyclical relationships
        final String name = this.getEntity(entityClass);
        final EntityMeta meta = new EntityMeta(name, entityClass);
        this.entities.put(entityClass, meta);

        // configure entity
        this.readFirstPass(meta);
        this.readIdAccessor(meta);

        // done
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


    protected String getTable(final EntityMeta entity, final Class<?> clazz)
    {
        // get default table name for entity
        String tableName = CaseUtils.camelToSnakeCase(entity.getType().getSimpleName());
        for (final Class<?> hierarchyClass : this.getHierarchy(entity.getType()))
        {
            final Table table = hierarchyClass.getAnnotation(Table.class);
            if (table != null)
            {
                tableName = table.name();
                break;
            }
        }
        if (clazz.equals(entity.getType()))
        {
            return tableName;
        }

        // first determine if we have an inherited entity split across a table
        final Inheritance inheritance = this.findAnnotation(entity.getClass(), Inheritance.class);
        if (null == inheritance || null == inheritance.strategy() || !InheritanceType.JOINED.equals(inheritance.strategy()))
        {
            return tableName;
        }

        // determine if we are looking at base table
        String baseName = tableName;
        for (final Class<?> hierarchyClass : this.getHierarchy(entity.getType()))
        {
            final Table table = hierarchyClass.getAnnotation(Table.class);
            if (table != null)
            {
                baseName = table.name();
            }
            if (hierarchyClass.equals(clazz))
            {
                return baseName;
            }
        }
        return baseName;
    }


    public Map<Field, GeneratedValue> getGenerators(final Class<?> entityClass)
    {
        final Map<Field, GeneratedValue> map = new HashMap<>();
        for (final Class<?> clazz : this.getHierarchy(entityClass))
        {
            for (final Field field : this.getFields(clazz))
            {
                final GeneratedValue annotation = field.getAnnotation(GeneratedValue.class);
                if (annotation != null)
                {
                    map.put(field, annotation);
                }
            }
        }
        return Collections.unmodifiableMap(map);
    }


    public <T extends Annotation> T findAnnotation(final Class<?> entityClass, final Class<T> clazz)
    {
        final List<T> list = this.findAnnotations(entityClass, clazz);
        if (null == list || list.isEmpty())
        {
            return null;
        }
        return list.get(0);
    }


    public <T extends Annotation> List<T> findAnnotations(final Class<?> entityClass, final Class<T> clazz)
    {
        final List<T> list = new ArrayList<>();
        for (final Class<?> type : this.getHierarchy(entityClass))
        {
            final T classAnnotation = type.getAnnotation(clazz);
            if (classAnnotation != null)
            {
                list.add(classAnnotation);
            }
            for (final Field field : this.getFields(type))
            {
                final T fieldAnnotation = field.getAnnotation(clazz);
                if (fieldAnnotation != null)
                {
                    list.add(fieldAnnotation);
                }
            }
        }
        return Collections.unmodifiableList(list);
    }


    /**
     * read secondary attributes, associations
     */
    private boolean readSecondPass(final EntityMeta entity)
    {
        if (null == entity)
        {
            return false;
        }

        // read secondary columns
        return this.readFields(entity, this.jpaAnnotations()) > 0;
    }


    /**
     * read core id / primary key attributes
     */
    private boolean readFirstPass(final EntityMeta entity)
    {
        if (null == entity)
        {
            return false;
        }

        // read all id columns
        final List<Class<? extends Annotation>> list = new ArrayList<>(2);
        list.add(EmbeddedId.class);
        list.add(Id.class);
        this.readFields(entity, list);

        // read hierarchy type discriminator (if applicable)
        final Class<?> entityClazz = entity.getType();
        final DiscriminatorMeta discriminator = this.getDiscriminator(entityClazz);
        if (discriminator != null)
        {
            // find class that has table
            String tableName = this.getTable(entity, entityClazz);
            for (final Class<?> clazz : this.getHierarchy(entityClazz))
            {
                final Inheritance inheritance = clazz.getAnnotation(Inheritance.class);
                if (inheritance != null && !InheritanceType.TABLE_PER_CLASS.equals(inheritance.strategy()))
                {
                    tableName = this.getTable(entity, clazz);
                    break;
                }
            }
            TableMeta table = entity.getTable(tableName);
            if (null == table)
            {
                table = new TableMeta(tableName, false);
                entity.addTable(table);
            }
            table.addColumn(discriminator);
            entity.setAccessor(discriminator, new ReadOnlyColumnAccessor(discriminator.getValue()));
        }

        return true;
    }


    private int readFields(final EntityMeta entity, final Collection<Class<? extends Annotation>> annotations)
    {
        if (null == entity)
        {
            return 0;
        }

        int num = 0;
        for (final Class<?> clazz : this.getHierarchy(entity.getType()))
        {
            // ensure table exists
            final String tableName = this.getTable(entity, clazz);
            TableMeta table = entity.getTable(tableName);
            if (null == table)
            {
                table = new TableMeta(tableName, false);
                entity.addTable(table);
            }

            // read fields
            for (final Field field : this.getFields(clazz, annotations))
            {
                final String name = this.getColumnName(field);
                if (!table.hasColumn(name))
                {
                    if (this.configureField(entity, table, field))
                    {
                        logger.debug("Configured metadata for [" + field + "] in [" + entity + "].");
                        num++;
                    }
                    else
                    {
                        logger.warn("Unable to configure metadata for [" + field + "] in [" + entity + "].");
                    }
                }
            }
        }
        return num;
    }


    private boolean readIdAccessor(final EntityMeta entity)
    {
        // find id fields
        final Class<?> entityClass = entity.getType();
        final List<Field> embeddedKeys = new ArrayList<>();
        final List<Field> regularKeys = new ArrayList<>();
        for (final Class<?> clazz : this.getHierarchy(entityClass))
        {
            for (final Field field : this.getFields(clazz))
            {
                if (field.getAnnotation(EmbeddedId.class) != null)
                {
                    embeddedKeys.add(field);
                }
                else if (field.getAnnotation(Id.class) != null)
                {
                    regularKeys.add(field);
                }
            }
        }

        if (embeddedKeys.isEmpty() && regularKeys.isEmpty())
        {
            return false;
        }
        if (!embeddedKeys.isEmpty() && !regularKeys.isEmpty())
        {
            throw new IllegalStateException("Cannot specify both @Id or @EmbeddedId annotations for class [" + entityClass + "].");
        }
        if (embeddedKeys.size() > 1 || regularKeys.size() > 1)
        {
            logger.warn("Multiple @Id or @EmbeddedId annotations found for class [" + entityClass + "] - you may be unable to access entity by #get api.");
            entity.setId(NullIdAccessor.getInstance());
            return true;
        }

        if (embeddedKeys.size() > 0)
        {
            // composite keys
            final Field key = embeddedKeys.get(0);
            final Map<ColumnMeta, ColumnAccessor> map = this.configureId(key, false);
            entity.setId(new CompositeIdAccessor(key, map));
            return true;
        }
        else if (regularKeys.size() > 0)
        {
            // regular primary key
            final Field key = regularKeys.get(0);
            for (final ColumnMeta column : this.configureId(key, false).keySet())
            {
                entity.setId(new BasicIdAccessor(key, column.getName()));
                return true;
            }
        }

        // no id configured
        return false;
    }


    private String getColumnName(final Field field)
    {
        // basic column info
        final Column column = field.getAnnotation(Column.class);
        final JoinColumn join = field.getAnnotation(JoinColumn.class);
        String name = CaseUtils.camelToSnakeCase(field.getName());
        if (column != null && !column.name().trim().isEmpty())
        {
            name = column.name();
        }
        else if (join != null && !join.name().trim().isEmpty())
        {
            name = join.name();
        }
        return name;
    }


    private Map<ColumnMeta, ColumnAccessor> configureId(final Field field, final boolean useNested)
    {
        final Class<?> type = field.getType();
        final Id id = field.getAnnotation(Id.class);
        if (id != null)
        {
            final String name = this.getColumnName(field);
            final String property = field.getName();
            final ColumnMeta column = new ColumnMeta(name, property, type, true, false);
            final ColumnAccessor accessor = new BasicColumnAccessor(field, type);
            final Map<ColumnMeta, ColumnAccessor> map = new HashMap<>(1);
            map.put(column, accessor);
            return Collections.unmodifiableMap(map);
        }

        final EmbeddedId embeddedId = field.getAnnotation(EmbeddedId.class);
        if (embeddedId != null)
        {
            final Embeddable embeddable = type.getAnnotation(Embeddable.class);
            if (null == embeddable)
            {
                throw new IllegalStateException("Class [" + type + "] does not have Embeddable annotation.");
            }
            final Map<ColumnMeta, ColumnAccessor> map = new HashMap<>();
            for (final Field embeddedColumn : new AnnotationParser(type).getFields(type))
            {
                final Class<?> embeddedClass = embeddedColumn.getType();
                final String embeddedName = this.getColumnName(embeddedColumn);
                final String property = field.getName() + "." + embeddedColumn.getName();
                final ColumnMeta column = new ColumnMeta(embeddedName, property, embeddedClass, true, false);
                final ColumnAccessor basic = new BasicColumnAccessor(embeddedColumn, embeddedClass);
                final ColumnAccessor accessor;
                if (useNested)
                {
                    accessor = new NestedColumnAccessor(field, basic);
                }
                else
                {
                    accessor = basic;
                }
                map.put(column, accessor);
            }
            return Collections.unmodifiableMap(map);
        }

        return Collections.emptyMap();
    }


    private boolean configureField(final EntityMeta entity, final TableMeta table, final Field field)
    {
        // basic column info
        final String name = this.getColumnName(field);
        final Class<?> type = field.getType();

        // primary key
        if (field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(EmbeddedId.class))
        {
            return entity.putColumns(table, this.configureId(field, true));
        }

        // regular column
        if (field.isAnnotationPresent(ElementCollection.class))
        {
            return entity.putColumns(table, this.configureElementCollection(field, name, field.getName()));
        }

        // associations and join columns
        if (field.isAnnotationPresent(OneToOne.class))
        {
            return entity.putColumns(table, this.configureOneToOne(field, name, field.getName()));
        }
        else if (field.isAnnotationPresent(ManyToOne.class))
        {
            return entity.putColumns(table, this.configureManyToOne(field, name, field.getName()));
        }
        else if (field.isAnnotationPresent(OneToMany.class))
        {
            return this.configureOneToMany(entity, field, name, field.getName());
        }

        // regular column
        if (field.getAnnotation(Column.class) != null)
        {
            final ColumnAccessor accessor = new BasicColumnAccessor(field, type);
            final ColumnMeta column = new ColumnMeta(name, field.getName(), type, false, false);
            table.addColumn(column);
            entity.setAccessor(column, accessor);
            return true;
        }

        // unsupported column
        return false;
    }


    private Map<ColumnMeta, ColumnAccessor> configureOneToOne(final Field field, final String name, final String property)
    {
        final OneToOne oneToOne = field.getAnnotation(OneToOne.class);
        final boolean lazy = FetchType.LAZY.equals(oneToOne.fetch());
        final String mappedBy = oneToOne.mappedBy().trim();
        if (mappedBy.isEmpty())
        {
            // create table column for this relationship
            final Class<?> type = field.getType();
            final EntityMeta entity = this.readEntity(type);
            if (null == entity)
            {
                throw new IllegalStateException("Type [" + type + "] is not a registered entity.");
            }
            final ColumnAccessor accessor = new SingleJoinColumnAccessor(field, entity, lazy);
            final ColumnMeta primary = entity.getPrimaryKeys().iterator().next();
            final ColumnMeta column = new JoinColumnMeta(name, property, primary.getType(), entity, false);
            final Map<ColumnMeta, ColumnAccessor> map = new HashMap<>(1);
            map.put(column, accessor);
            return Collections.unmodifiableMap(map);
        }
        else
        {
            // this table does not own this column
            return Collections.emptyMap();
        }
    }


    private Map<ColumnMeta, ColumnAccessor> configureManyToOne(final Field field, final String name, final String property)
    {
        final ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
        final boolean lazy = FetchType.LAZY.equals(manyToOne.fetch());
        final Class<?> type = field.getType();
        final EntityMeta entity = this.readEntity(type);
        if (null == entity)
        {
            throw new IllegalStateException("Type [" + type + "] is not a registered entity.");
        }
        final ColumnAccessor accessor = new SingleJoinColumnAccessor(field, entity, lazy);
        final ColumnMeta primary = entity.getPrimaryKeys().iterator().next();
        final ColumnMeta column = new JoinColumnMeta(name, property, primary.getType(), entity, false);
        final Map<ColumnMeta, ColumnAccessor> map = new HashMap<>(1);
        map.put(column, accessor);
        return Collections.unmodifiableMap(map);
    }


    private boolean configureOneToMany(final EntityMeta parentEntity, final Field field, final String name, final String property)
    {
        // grab basic one-to-many parameters
        final OneToMany oneToMany = field.getAnnotation(OneToMany.class);
        final boolean lazy = FetchType.LAZY.equals(oneToMany.fetch());
        final Class<?> parameterizedClass;
        if (oneToMany.targetEntity() != null && !void.class.equals(oneToMany.targetEntity()))
        {
            parameterizedClass = oneToMany.targetEntity();
        }
        else
        {
            final ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
            final Type[] types = parameterizedType != null ? parameterizedType.getActualTypeArguments() : null;
            parameterizedClass = types != null && types.length > 0 ? (Class<?>) types[0] : null;
        }
        if (null == parameterizedClass)
        {
            return false;
        }

        final EntityMeta associatedEntity = this.readEntity(parameterizedClass);
        if (null == associatedEntity)
        {
            throw new IllegalStateException("Type [" + parameterizedClass + "] is not a registered entity.");
        }

        // determine if we are using a join table
        final JoinTable joinTable = field.getAnnotation(JoinTable.class);
        if (joinTable != null)
        {
            // we query the columns from a auxiliary join table
            final String tableName;
            if (joinTable.name().isEmpty())
            {
                tableName = CaseUtils.camelToSnakeCase(parentEntity.getName()) + "_" + CaseUtils.camelToSnakeCase(property);
            }
            else
            {
                tableName = joinTable.name();
            }
            TableMeta join = parentEntity.getTable(tableName);
            if (null == join)
            {
                join = new TableMeta(tableName, true);
                parentEntity.addTable(join);
            }
            for (final ColumnMeta primary : parentEntity.getPrimaryKeys())
            {
                join.addColumn(primary);
            }
            final ColumnMeta primary = associatedEntity.getPrimaryKeys().iterator().next();
            final ColumnMeta column = new JoinCollectionMeta(name, property, primary.getType(), associatedEntity, true, lazy);
            final ColumnAccessor accessor = new ManyJoinColumnAccessor(field, associatedEntity, lazy);
            join.addColumn(column);
            parentEntity.setAccessor(column, accessor);
            return true;
        }
        else if (oneToMany.mappedBy() != null && !oneToMany.mappedBy().trim().isEmpty())
        {
            // we query this value via the other side of the relationship (which would require indices)
            return false;
        }
        else
        {
            // use embedded collection
            return false;
        }
    }


    private Map<ColumnMeta, ColumnAccessor> configureElementCollection(final Field field, final String name, final String property)
    {
        final ElementCollection annotation = field.getAnnotation(ElementCollection.class);
        final boolean lazy = annotation != null && FetchType.LAZY.equals(annotation.fetch());
        final Class<?> type = field.getType();
        final ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
        final Type[] types = parameterizedType.getActualTypeArguments();
        final Class<?> parameterizedClass = types != null && types.length > 0 ? (Class<?>) types[0] : null;
        if (null == parameterizedClass)
        {
            return Collections.emptyMap();
        }

        if (this.isEntity(parameterizedClass))
        {
            return Collections.emptyMap();
        }

        final ColumnAccessor accessor;
        if (type.isInstance(Set.class))
        {
            accessor = new SetColumnAccessor(field, parameterizedClass, lazy);
        }
        else
        {
            accessor = new ListColumnAccessor(field, parameterizedClass, lazy);
        }
        final Map<ColumnMeta, ColumnAccessor> map = new HashMap<>(1);
        final ColumnMeta column = new CollectionMeta(name, property, type, parameterizedClass, false, lazy);
        map.put(column, accessor);
        return Collections.unmodifiableMap(map);
    }


    protected DiscriminatorMeta getDiscriminator(final Class<?> entityClass)
    {
        // first look for discriminator annotation
        DiscriminatorColumn column = null;
        for (final Class<?> clazz : this.getHierarchy(entityClass))
        {
            column = clazz.getAnnotation(DiscriminatorColumn.class);
            if (column != null)
            {
                break;
            }
        }
        if (null == column || null == column.discriminatorType())
        {
            return null;
        }

        // next get discriminator value
        String discriminator = CaseUtils.camelToSnakeCase(entityClass.getSimpleName());
        for (final Class<?> clazz : this.getHierarchy(entityClass))
        {
            final DiscriminatorValue value = clazz.getAnnotation(DiscriminatorValue.class);
            if (value != null)
            {
                discriminator = value.value();
                break;
            }
        }
        final String name = column.name();
        final String property = "DiscriminatorColumn";
        switch (column.discriminatorType())
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


    private List<Field> getFields(final Class<?> clazz, final Collection<Class<? extends Annotation>> annotations)
    {
        if (null == clazz)
        {
            return Collections.emptyList();
        }

        final Field[] fields = clazz.getDeclaredFields();
        if (null == fields || fields.length <= 0)
        {
            return Collections.emptyList();
        }

        final List<Field> list = new ArrayList<>(fields.length);
        for (final Field field : fields)
        {
            if (!Modifier.isTransient(field.getModifiers()))
            {
                for (final Class<? extends Annotation> annotation : annotations)
                {
                    if (field.isAnnotationPresent(annotation))
                    {
                        list.add(field);
                    }
                }
            }
        }
        return list;
    }


    private List<Field> getFields(final Class<?> clazz)
    {
        return this.getFields(clazz, this.jpaAnnotations());
    }


    private Collection<Class<? extends Annotation>> jpaAnnotations()
    {
        final List<Class<? extends Annotation>> list = new ArrayList<>();
        list.add(Column.class);
        list.add(EmbeddedId.class);
        list.add(Id.class);
        list.add(ElementCollection.class);
        list.add(JoinColumn.class);
        list.add(OneToMany.class);
        list.add(ManyToOne.class);
        list.add(ManyToMany.class);
        list.add(OneToOne.class);
        return Collections.unmodifiableList(list);
    }


    private List<Class<?>> getHierarchy(final Class<?> entityClass)
    {
        if (null == entityClass || Object.class.equals(entityClass))
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
}
