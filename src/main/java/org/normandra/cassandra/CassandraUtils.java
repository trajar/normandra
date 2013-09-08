package org.normandra.cassandra;

import org.normandra.config.CollectionMeta;
import org.normandra.config.ColumnMeta;

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
            if (Collection.class.equals(clazz) || List.class.equals(clazz))
            {
                return "list<" + type + ">";
            }
            if (Set.class.equals(clazz))
            {
                return "set<" + type + ">";
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


    public static String columnType(final Class<?> clazz)
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
        // arbitrary bytes (no validation), expressed as hexadecimal
        return "blob";
    }


    private CassandraUtils()
    {

    }
}
