package org.normandra.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.normandra.meta.CollectionMeta;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.JoinCollectionMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * collection of common orient utilities
 * <p/>
 * User: bowen
 * Date: 5/15/14
 */
public class OrientUtils
{
    public static String keyIndex(final EntityMeta meta)
    {
        if (null == meta)
        {
            return null;
        }

        final String entity = meta.getInherited() != null ? meta.getInherited() : meta.getName();
        final Collection<ColumnMeta> keys = new SingleEntityContext(meta).getPrimaryKeys();
        if (keys.size() == 1)
        {
            final ColumnMeta key = keys.iterator().next();
            return entity + "." + key.getName();
        }
        else
        {
            return entity + ".key";
        }
    }


    public static Object unpackValue(final EntityContext context, final ODocument document, final ColumnMeta column)
    {
        if (null == context || null == document || null == column)
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

        if (column instanceof JoinCollectionMeta)
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


    private static Object unpackPrimitive(final Class<?> clazz, final Object value)
    {
        if (UUID.class.equals(clazz))
        {
            final byte[] data = (byte[]) value;
            if (data.length <= 0)
            {
                return null;
            }
            return UUIDSerializer.instance.deserialize(ByteBuffer.wrap(data));
        }
        if (InetAddress.class.equals(clazz))
        {
            final byte[] data = (byte[]) value;
            if (data.length <= 0)
            {
                return null;
            }
            return InetAddressSerializer.instance.deserialize(ByteBuffer.wrap(data));
        }

        return value;

    }


    static Object packRaw(final ColumnMeta column, final Object value)
    {
        if (null == column || null == value)
        {
            return null;
        }

        final Class<?> clazz = column.getType();

        if (column instanceof JoinCollectionMeta)
        {
            final Collection list = (Collection) value;
            final List packed = new ArrayList<>(list.size());
            for (final Object item : list)
            {
                final Object pack = packPrimitive(clazz, item);
                packed.add(pack);
            }
            return packed;
        }

        if (List.class.isAssignableFrom(clazz))
        {
            return new ArrayList((Collection) value);
        }
        if (Collection.class.isAssignableFrom(clazz))
        {
            return new ArraySet((Collection) value);
        }

        return packPrimitive(clazz, value);
    }


    static Object packPrimitive(final Class<?> clazz, final Object value)
    {
        if (UUID.class.equals(clazz))
        {
            return UUIDSerializer.instance.serialize((UUID) value).array();
        }
        if (InetAddress.class.equals(clazz))
        {
            return InetAddressSerializer.instance.serialize((InetAddress) value).array();
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
        if (null == column)
        {
            return null;
        }

        final Class<?> clazz = column.getType();

        // handle collections
        if (column instanceof CollectionMeta)
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
        else if (column instanceof JoinCollectionMeta)
        {
            return OType.EMBEDDEDSET;
        }
        else if (List.class.isAssignableFrom(clazz))
        {
            return OType.LINKLIST;
        }
        else if (Collection.class.isAssignableFrom(clazz))
        {
            return OType.LINKSET;
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


    private OrientUtils()
    {
    }
}
