package org.normandra.util;

import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;

/**
 * common data utilities
 * <p>
 * User: bowen
 * Date: 7/14/14
 */
public class DataUtils
{
    public static byte[] inetToBytes(final InetAddress inet)
    {
        if (null == inet)
        {
            return new byte[0];
        }
        final ByteBuffer buffer = InetAddressSerializer.instance.serialize(inet);
        if (null == buffer)
        {
            return new byte[0];
        }
        return buffer.array();
    }


    public static String inetToString(final InetAddress inet)
    {
        final byte[] data = inetToBytes(inet);
        if (null == data || data.length <= 0)
        {
            return null;
        }
        return Base64.getMimeEncoder().encodeToString(data);
    }


    public static InetAddress stringToInet(final String value)
    {
        if (null == value || value.isEmpty())
        {
            return null;
        }
        final byte[] data = Base64.getMimeDecoder().decode(value);
        return bytesToInet(data);
    }


    public static InetAddress bytesToInet(final byte[] data)
    {
        if (null == data || data.length <= 0)
        {
            return null;
        }
        return InetAddressSerializer.instance.deserialize(ByteBuffer.wrap(data));
    }


    public static byte[] uuidToBytes(final UUID uuid)
    {
        if (null == uuid)
        {
            return new byte[0];
        }
        final ByteBuffer buffer = UUIDSerializer.instance.serialize(uuid);
        if (null == buffer)
        {
            return new byte[0];
        }
        return buffer.array();
    }


    public static UUID bytesToUUID(final byte[] data)
    {
        if (null == data || data.length <= 0)
        {
            return null;
        }
        return UUIDSerializer.instance.deserialize(ByteBuffer.wrap(data));
    }


    public static String uuidToString(final UUID uuid)
    {
        final byte[] data = uuidToBytes(uuid);
        if (null == data || data.length <= 0)
        {
            return null;
        }
        return Base64.getMimeEncoder().encodeToString(data);
    }


    public static UUID stringToUUID(final String value)
    {
        if (null == value || value.isEmpty())
        {
            return null;
        }
        final byte[] data = Base64.getMimeDecoder().decode(value);
        return bytesToUUID(data);
    }


    public static Date longToDate(final Long value)
    {
        if (null == value)
        {
            return null;
        }
        return new Date(value.longValue());
    }


    public static Long dateToLong(final Date date)
    {
        if (null == date)
        {
            return null;
        }
        return date.getTime();
    }


    private DataUtils()
    {

    }
}
