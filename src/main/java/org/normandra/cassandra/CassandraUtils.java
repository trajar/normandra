package org.normandra.cassandra;

import com.datastax.driver.core.Row;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.normandra.meta.CollectionMeta;
import org.normandra.meta.ColumnMeta;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * cassandra utility methods
 * <p/>
 * User: bowen
 * Date: 9/7/13
 */
public class CassandraUtils
{
    public static Object unpack(final Row row, final String column, final ColumnMeta<?> meta) throws IOException, ClassNotFoundException
    {
        if (row.isNull(column))
        {
            return null;
        }

        if (meta instanceof CollectionMeta)
        {
            return unpackCollection(row, column, (CollectionMeta) meta);
        }

        final Class<?> clazz = meta.getType();
        if (String.class.equals(clazz))
        {
            return row.getString(column);
        }
        if (long.class.equals(clazz) || Long.class.equals(clazz))
        {
            return row.getLong(column);
        }
        if (int.class.equals(clazz) || Integer.class.equals(clazz))
        {
            return row.getInt(column);
        }
        if (double.class.equals(clazz) || Double.class.equals(clazz))
        {
            return row.getDouble(column);
        }
        if (float.class.equals(clazz) || Float.class.equals(clazz))
        {
            return row.getFloat(column);
        }
        if (boolean.class.equals(clazz) || Boolean.class.equals(clazz))
        {
            return row.getBool(column);
        }
        if (Date.class.equals(clazz))
        {
            return row.getDate(column);
        }
        if (UUID.class.equals(clazz))
        {
            return row.getUUID(column);
        }
        if (InetAddress.class.equals(clazz))
        {
            return row.getInet(column);
        }

        return unpackSerialized(row, column, meta);
    }


    private static Object unpackSerialized(final Row row, final String column, final ColumnMeta<?> meta) throws IOException, ClassNotFoundException
    {
        final ByteBuffer buffer = row.getBytes(column);
        if (null == buffer)
        {
            return null;
        }

        final ClassLoader cl = meta.getType().getClassLoader();
        final ObjectInputStream input = new ClassLoaderObjectInputStream(cl, new ByteArrayInputStream(buffer.array()));
        try
        {
            return input.readObject();
        }
        finally
        {
            IOUtils.closeQuietly(input);
        }
    }


    private static Object unpackCollection(final Row row, final String column, final CollectionMeta<?> meta)
    {
        final Class<?> clazz = meta.getType();
        if (Set.class.isAssignableFrom(clazz))
        {
            return row.getSet(column, meta.getGeneric());
        }
        else if (Collection.class.isAssignableFrom(clazz) || List.class.isAssignableFrom(clazz))
        {
            return row.getList(column, meta.getGeneric());
        }
        else
        {
            throw new UnsupportedOperationException("Unable to unpack cassandra collection of type [" + clazz + "].");
        }
    }


    public static String columnType(final ColumnMeta column)
    {
        if (null == column)
        {
            return null;
        }
        final Class<?> clazz = column.getType();
        if (column instanceof CollectionMeta)
        {
            final CollectionMeta collection = (CollectionMeta) column;
            final Class<?> generic = collection.getGeneric();
            final String type = columnType(generic);
            if (Set.class.isAssignableFrom(clazz))
            {
                return "set<" + type + ">";
            }
            else if (Collection.class.isAssignableFrom(clazz) || List.class.isAssignableFrom(clazz))
            {
                return "list<" + type + ">";
            }
            else
            {
                throw new UnsupportedOperationException("Unable to construct cassandra collection of type [" + clazz + "].");
            }
        }
        else
        {
            return columnType(clazz);
        }
    }


    private static String columnType(final Class<?> clazz)
    {
        if (null == clazz)
        {
            return null;
        }
        if (String.class.equals(clazz))
        {
//          return "ascii";
//          return "varchar";
            return "text";
        }
        if (long.class.equals(clazz) || Long.class.equals(clazz))
        {
            return "bigint";
        }
        if (int.class.equals(clazz) || Integer.class.equals(clazz))
        {
            return "int";
        }
        if (double.class.equals(clazz) || Double.class.equals(clazz))
        {
            return "double";
        }
        if (float.class.equals(clazz) || Float.class.equals(clazz))
        {
            return "float";
        }
        if (boolean.class.equals(clazz) || Boolean.class.equals(clazz))
        {
            return "boolean";
        }
        if (Date.class.equals(clazz))
        {
            return "timestamp";
        }
        if (UUID.class.equals(clazz))
        {
            return "uuid";
        }
        if (InetAddress.class.equals(clazz))
        {
            return "inet";
        }
        // arbitrary bytes (no validation), expressed as hexadecimal
        return "blob";
    }


    private CassandraUtils()
    {

    }
}
