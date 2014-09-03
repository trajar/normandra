package org.normandra.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DiscriminatorMeta;
import org.normandra.meta.EmbeddedCollectionMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;
import org.normandra.util.DataUtils;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * collection of common orient utilities
 * <p>
 * User: bowen
 * Date: 5/15/14
 */
public class OrientUtils
{
    public static Object unpackValue(final ODocument document, final ColumnMeta column)
    {
        if (null == document || null == column)
        {
            return null;
        }

        final String name = column.getName();
        final Object raw = document.field(name);
        if (null == raw)
        {
            return null;
        }
        return OrientUtils.unpackRaw(column, raw);
    }


    public static Object unpackKey(final EntityContext context, final ODocument document)
    {
        if (null == context || null == document)
        {
            return null;
        }

        final Collection<ColumnMeta> keys = context.getPrimaryKeys();
        final Set<ColumnMeta> columns = new ArraySet<>(keys.size() + 1);
        columns.addAll(keys);
        for (final EntityMeta meta : context.getEntities())
        {
            final DiscriminatorMeta descrim = meta.getDiscriminator();
            if (descrim != null)
            {
                columns.add(descrim.getColumn());
            }
        }

        final Map<String, Object> data = new TreeMap<>();
        for (final ColumnMeta column : columns)
        {
            final Object value = unpackValue(document, column);
            if (value != null)
            {
                data.put(column.getName(), value);
            }
        }
        return context.getId().toKey(data);
    }


    public static Map<ColumnMeta, Object> unpackValues(final EntityContext context, final ODocument document)
    {
        if (null == context || null == document)
        {
            return Collections.emptyMap();
        }

        final Map<ColumnMeta, Object> data = new TreeMap<>();
        for (final TableMeta table : context.getPrimaryTables())
        {
            for (final ColumnMeta column : table.getColumns())
            {
                final String name = column.getName();
                final Object raw = document.field(name);
                final Object value = raw != null ? OrientUtils.unpackRaw(column, raw) : null;
                if (value != null)
                {
                    data.put(column, value);
                }
            }
        }
        return Collections.unmodifiableMap(data);
    }


    static Object unpackRaw(final ColumnMeta column, final Object value)
    {
        if (null == column || null == value)
        {
            return null;
        }

        final Class<?> clazz = column.getType();

        if (column.isCollection() && value instanceof Collection)
        {
            final Collection list = (Collection) value;
            final List packed = new ArrayList<>(list.size());
            for (final Object item : list)
            {
                final Object pack = unpackPrimitive(clazz, item);
                packed.add(pack);
            }
            return packed;
        }

        return unpackPrimitive(clazz, value);
    }


    static Object packRaw(final ColumnMeta column, final Object value)
    {
        if (null == column || null == value)
        {
            return null;
        }

        final Class<?> clazz = column.getType();

        if (column.isCollection() && value instanceof Collection)
        {
            final Collection list = (Collection) value;
            final List<Object> packed = new ArrayList<>(list.size());
            for (final Object item : list)
            {
                final Object pack = packPrimitive(clazz, item);
                packed.add(pack);
            }
            if (List.class.isAssignableFrom(clazz))
            {
                return new ArrayList<>(packed);
            }
            if (Collection.class.isAssignableFrom(clazz))
            {
                return new ArraySet<>(packed);
            }
        }

        return packPrimitive(clazz, value);
    }


    static Object unpackPrimitive(final Class<?> clazz, final Object value)
    {
        if (UUID.class.equals(clazz))
        {
            return DataUtils.bytesToUUID((byte[]) value);
        }
        if (InetAddress.class.equals(clazz))
        {
            return DataUtils.bytesToInet((byte[]) value);
        }

        return value;
    }


    static Object packPrimitive(final Class<?> clazz, final Object value)
    {
        if (UUID.class.equals(clazz))
        {
            return DataUtils.uuidToBytes((UUID) value);
        }
        if (InetAddress.class.equals(clazz))
        {
            return DataUtils.inetToBytes((InetAddress) value);
        }

        if (Number.class.isAssignableFrom(clazz) && !clazz.equals(value.getClass()))
        {
            final Number number = (Number) value;
            if (Long.class.equals(clazz))
            {
                return number.longValue();
            }
            else if (Integer.class.equals(clazz))
            {
                return number.intValue();
            }
            else if (Short.class.equals(clazz))
            {
                return number.shortValue();
            }
            else if (Float.class.equals(clazz))
            {
                return number.floatValue();
            }
            else if (Double.class.equals(clazz))
            {
                return number.doubleValue();
            }
        }

        return value;
    }


    public static OType columnType(final ColumnMeta column)
    {
        if (null == column || column.isVirtual())
        {
            return null;
        }

        final Class<?> clazz = column.getType();

        // handle collections
        if (column instanceof EmbeddedCollectionMeta)
        {
            if (List.class.isAssignableFrom(clazz))
            {
                return OType.EMBEDDEDLIST;
            }
            else if (Collection.class.isAssignableFrom(clazz))
            {
                return OType.EMBEDDEDSET;
            }
        }
        else if (List.class.isAssignableFrom(clazz))
        {
//          return OType.LINKLIST;
            return OType.EMBEDDEDLIST;
        }
        else if (Collection.class.isAssignableFrom(clazz))
        {
//          return OType.LINKSET;
            return OType.EMBEDDEDSET;
        }

        // handle regular values
        if (String.class.equals(clazz))
        {
            return OType.STRING;
        }
        if (long.class.equals(clazz) || Long.class.equals(clazz))
        {
            return OType.LONG;
        }
        if (int.class.equals(clazz) || Integer.class.equals(clazz))
        {
            return OType.INTEGER;
        }
        if (short.class.equals(clazz) || Short.class.equals(clazz))
        {
            return OType.SHORT;
        }
        if (double.class.equals(clazz) || Double.class.equals(clazz))
        {
            return OType.DOUBLE;
        }
        if (float.class.equals(clazz) || Float.class.equals(clazz))
        {
            return OType.FLOAT;
        }
        if (BigDecimal.class.equals(clazz))
        {
            return OType.DECIMAL;
        }
        if (boolean.class.equals(clazz) || Boolean.class.equals(clazz))
        {
            return OType.BOOLEAN;
        }
        if (Date.class.equals(clazz))
        {
            return OType.DATETIME;
        }
        return OType.BINARY;
    }


    static String keyIndex(final TableMeta table)
    {
        if (null == table)
        {
            return null;
        }

        final Collection<ColumnMeta> keys = table.getPrimaryKeys();
        if (keys.size() == 1)
        {
            final ColumnMeta key = keys.iterator().next();
            return table.getName() + "." + key.getName();
        }
        else
        {
            return table.getName() + ".key";
        }
    }


    private OrientUtils()
    {
    }
}
