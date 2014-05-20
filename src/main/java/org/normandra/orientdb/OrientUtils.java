package org.normandra.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
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
        return meta.getName() + "._key";
    }


    public static Object unpackValue(final EntityContext context, final ODocument document, final ColumnMeta column)
    {
        if (null == context || null == document || null == column)
        {
            return null;
        }

        final String name = column.getProperty();
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
                final String name = column.getProperty();
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

        if (UUID.class.equals(clazz))
        {
            return UUIDSerializer.instance.serialize((UUID) value).array();
        }
        if (InetAddress.class.equals(clazz))
        {
            return InetAddressSerializer.instance.serialize((InetAddress) value).array();
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

        if (List.class.isAssignableFrom(clazz))
        {
            return OType.LINKLIST;
        }
        if (List.class.isAssignableFrom(clazz))
        {
            return OType.LINKSET;
        }

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
