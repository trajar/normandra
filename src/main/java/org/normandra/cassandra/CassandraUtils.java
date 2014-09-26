package org.normandra.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.normandra.NormandraException;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DiscriminatorMeta;
import org.normandra.meta.EmbeddedCollectionMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.JoinCollectionMeta;
import org.normandra.meta.MappedColumnMeta;
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
 * <p>
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
                final Object value = CassandraUtils.unpackValue(row, column);
                if (value != null)
                {
                    data.put(column, value);
                }
            }
        }
        return Collections.unmodifiableMap(data);
    }


    public static Object unpackKey(final Row row, final EntityContext entity) throws NormandraException, IOException, ClassNotFoundException
    {
        if (null == row || null == entity)
        {
            return null;
        }

        final Collection<ColumnMeta> keys = entity.getPrimaryKeys();
        final Set<ColumnMeta> columns = new ArraySet<>(keys.size() + 1);
        columns.addAll(keys);
        for (final EntityMeta meta : entity.getEntities())
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
            final Object value = unpackValue(row, column);
            if (value != null)
            {
                data.put(column.getName(), value);
            }
        }
        return entity.getId().toKey(data);
    }


    public static Object unpackValue(final Row row, final ColumnMeta column) throws IOException, ClassNotFoundException
    {
        if (null == column)
        {
            return null;
        }

        final String columnName = column.getName();

        if (null == row || row.isNull(columnName))
        {
            return null;
        }

        if (column.isCollection() && column.isEmbedded())
        {
            final Class<?> generic;
            if (column instanceof EmbeddedCollectionMeta)
            {
                generic = ((EmbeddedCollectionMeta) column).getGeneric();
            }
            else if (column instanceof JoinCollectionMeta)
            {
                generic = ((JoinCollectionMeta) column).getEntity().getPrimaryKey().getType();
            }
            else
            {
                throw new IllegalArgumentException("Unable to build column type for [" + column + "].");
            }
            return unpackCollection(row, column.getType(), generic, columnName);
        }

        final DataType type = row.getColumnDefinitions().getType(columnName);

        if (DataType.text().equals(type) || DataType.varchar().equals(type) || DataType.ascii().equals(type))
        {
            return row.getString(columnName);
        }
        else if (DataType.bigint().equals(type))
        {
            return row.getLong(columnName);
        }
        else if (DataType.cint().equals(type))
        {
            return row.getInt(columnName);
        }
        else if (DataType.cdouble().equals(type))
        {
            return row.getDouble(columnName);
        }
        else if (DataType.cfloat().equals(type))
        {
            return row.getFloat(columnName);
        }
        else if (DataType.cboolean().equals(type))
        {
            return row.getBool(columnName);
        }
        else if (DataType.timestamp().equals(type))
        {
            return row.getDate(columnName);
        }
        else if (DataType.uuid().equals(type))
        {
            return row.getUUID(columnName);
        }
        else if (DataType.inet().equals(type))
        {
            return row.getInet(columnName);
        }
        else
        {
            return unpackSerialized(row, columnName, column);
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


    private static Collection<?> unpackCollection(final Row row, final Class<?> type, final Class<?> generic, final String columnName)
    {
        if (List.class.isAssignableFrom(type))
        {
            if (null == row || row.isNull(columnName))
            {
                return Collections.emptyList();
            }
            else
            {
                return row.getList(columnName, generic);
            }
        }
        else
        {
            if (null == row || row.isNull(columnName))
            {
                return Collections.emptySet();
            }
            else
            {
                return row.getSet(columnName, generic);
            }
        }
    }


    public static String columnType(final ColumnMeta column)
    {
        if (null == column)
        {
            return null;
        }
        if (column instanceof MappedColumnMeta)
        {
            return null;
        }

        final Class<?> clazz = column.getType();

        if (column.isCollection() && column.isEmbedded())
        {
            final Class<?> generic;
            if (column instanceof EmbeddedCollectionMeta)
            {
                generic = ((EmbeddedCollectionMeta) column).getGeneric();
            }
            else if (column instanceof JoinCollectionMeta)
            {
                generic = ((JoinCollectionMeta) column).getEntity().getPrimaryKey().getType();
            }
            else
            {
                throw new IllegalArgumentException("Unable to build column type for [" + column + "].");
            }
            final String type = columnType(generic);
            if (List.class.isAssignableFrom(clazz))
            {
                return "list<" + type + ">";
            }
            else
            {
                return "set<" + type + ">";
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
