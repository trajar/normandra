package org.normandra.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
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
import org.normandra.util.DataUtils;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.ByteBuffer;
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
 * cassandra utility methods
 * <p>
 * 
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
                data.put(column, value);
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


    public static Object packValue(final ColumnMeta column, final Object value)
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
                final Object pack = packPrimitive(item);
                if (pack != null)
                {
                    packed.add(pack);
                }
            }
            if (List.class.isAssignableFrom(clazz))
            {
                return new ArrayList<>(packed);
            }
            else if (Collection.class.isAssignableFrom(clazz))
            {
                return new ArraySet<>(packed);
            }
        }

        return packPrimitive(value);
    }


    private static Object packPrimitive(final Object value)
    {
        if (null == value)
        {
            return null;
        }
        if (value instanceof Date)
        {
            return value;
        }
        if (value instanceof String)
        {
            return value;
        }
        if (value instanceof Number)
        {
            return value;
        }
        if(value instanceof Boolean)
        {
            return value;
        }
        if (value instanceof UUID)
        {
            return value;
        }
        if (value instanceof InetAddress)
        {
            return value;
        }
        if (byte[].class.equals(value.getClass()))
        {
            final byte[] data = (byte[]) value;
            if (data.length <= 0)
            {
                return null;
            }
            return ByteBuffer.wrap(data);
        }
        if (value instanceof Serializable)
        {
            final byte[] data = DataUtils.objectToBytes((Serializable) value);
            if (null == data || data.length <= 0)
            {
                return null;
            }
            return ByteBuffer.wrap(data);
        }

        throw new IllegalArgumentException("Unexpected value [" + value + "].");
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
            final ByteBuffer buffer = row.getBytes(columnName);
            if (null == buffer)
            {
                return null;
            }
            final Class serialized = column.getType();
            return DataUtils.bytesToObject(serialized, buffer.array());
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
