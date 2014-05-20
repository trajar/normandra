package org.normandra.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.normandra.NormandraException;
import org.normandra.meta.CollectionMeta;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.JoinCollectionMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * cassandra utility methods
 * <p/>
 * User: bowen
 * Date: 9/7/13
 */
public class CassandraUtils
{
    public static Map<ColumnMeta, Object> unpackValues(final TableMeta table, final Row row) throws IOException, ClassNotFoundException, NormandraException
    {
        if (null == row || null == table)
        {
            return Collections.emptyMap();
        }
        final Map<ColumnMeta, Object> data = new TreeMap<>();
        for (final ColumnDefinitions.Definition def : row.getColumnDefinitions().asList())
        {
            final String columnName = def.getName();
            final ColumnMeta column = table.getColumn(columnName);
            if (column != null)
            {
                final Object value = CassandraUtils.unpackValue(row, columnName, column);
                if (value != null)
                {
                    data.put(column, value);
                }
            }
        }
        return Collections.unmodifiableMap(data);
    }


    public static Object unpackValue(final List<Row> rows, final String column, final ColumnMeta meta) throws IOException, ClassNotFoundException
    {
        if (null == rows || rows.isEmpty())
        {
            return null;
        }
        if (null == meta)
        {
            return null;
        }

        if (meta instanceof CollectionMeta)
        {
            return unpackCollection(rows.get(0), column, (CollectionMeta) meta);
        }
        else if (meta instanceof JoinCollectionMeta)
        {
            return unpackJoin(rows, column, (JoinCollectionMeta) meta);
        }
        else
        {
            final Row row = rows.get(0);
            if (null == row)
            {
                return null;
            }
            return unpackValue(row, column, meta);
        }

    }


    public static Object unpackValue(final Row row, final String column, final ColumnMeta meta) throws IOException, ClassNotFoundException
    {
        if (null == row || row.isNull(column))
        {
            return null;
        }
        if (null == meta)
        {
            return null;
        }

        if (meta instanceof CollectionMeta)
        {
            return unpackCollection(row, column, (CollectionMeta) meta);
        }

        final DataType type = row.getColumnDefinitions().getType(column);

        if (DataType.text().equals(type) || DataType.varchar().equals(type) || DataType.ascii().equals(type))
        {
            return row.getString(column);
        }
        else if (DataType.bigint().equals(type))
        {
            return row.getLong(column);
        }
        else if (DataType.cint().equals(type))
        {
            return row.getInt(column);
        }
        else if (DataType.cdouble().equals(type))
        {
            return row.getDouble(column);
        }
        else if (DataType.cfloat().equals(type))
        {
            return row.getFloat(column);
        }
        else if (DataType.cboolean().equals(type))
        {
            return row.getBool(column);
        }
        else if (DataType.timestamp().equals(type))
        {
            return row.getDate(column);
        }
        else if (DataType.uuid().equals(type))
        {
            return row.getUUID(column);
        }
        else if (DataType.inet().equals(type))
        {
            return row.getInet(column);
        }
        else
        {
            return unpackSerialized(row, column, meta);
        }
    }


    private static Object unpackSerialized(final Row row, final String column, final ColumnMeta meta) throws IOException, ClassNotFoundException
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


    private static Object unpackCollection(final Row row, final String column, final CollectionMeta meta)
    {
        final Class<?> clazz = meta.getType();
        if (Set.class.isAssignableFrom(clazz))
        {
            if (null == row || row.isNull(column))
            {
                return Collections.emptySet();
            }
            else
            {
                return row.getSet(column, meta.getGeneric());
            }
        }
        else if (Collection.class.isAssignableFrom(clazz) || List.class.isAssignableFrom(clazz))
        {
            if (null == row || row.isNull(column))
            {
                return Collections.emptyList();
            }
            else
            {
                return row.getList(column, meta.getGeneric());
            }
        }
        else
        {
            throw new UnsupportedOperationException("Unable to unpack cassandra collection of type [" + clazz + "].");
        }
    }


    private static Object unpackJoin(final Iterable<Row> results, final String column, final JoinCollectionMeta meta) throws IOException, ClassNotFoundException
    {
        final Set<Object> list = new ArraySet<>();
        for (final Row row : results)
        {
            final Object value = unpackValue(row, column, meta);
            if (value != null)
            {
                list.add(value);
            }
        }
        return list;
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
