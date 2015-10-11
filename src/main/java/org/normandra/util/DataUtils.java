package org.normandra.util;

import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

/**
 * common data utilities
 * <p>
 * 
 * Date: 7/14/14
 */
public class DataUtils
{
    private static final Logger logger = LoggerFactory.getLogger(DataUtils.class);


    public static byte[] objectToBytes(final Serializable value)
    {
        if (null == value)
        {
            return new byte[0];
        }
        try
        {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            final ObjectOutputStream output = new ObjectOutputStream(bytes);
            output.writeObject(value);
            IOUtils.closeQuietly(output);
            return bytes.toByteArray();
        }
        catch (final Exception e)
        {
            logger.warn("Unable to serialize value [" + value + "].", e);
            return new byte[0];
        }
    }


    public static <T extends Serializable> T bytesToObject(final Class<T> clazz, final byte[] value)
    {
        if (null == value || value.length <= 0)
        {
            return null;
        }
        try
        {
            final ByteArrayInputStream bytes = new ByteArrayInputStream(value);
            final ObjectInputStream input = new ClassLoaderObjectInputStream(clazz.getClassLoader(), bytes);
            final Object instance = input.readObject();
            IOUtils.closeQuietly(input);
            return clazz.cast(instance);
        }
        catch (final Exception e)
        {
            logger.warn("Unable to deserialize value [" + value + "] of type [" + clazz + "].", e);
            return null;
        }
    }


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


    public static Collection<Integer> fromIntArray(final int[] c)
    {
        if (null == c || c.length <= 0)
        {
            return Collections.emptyList();
        }
        final Collection<Integer> list = new ArrayList<>(c.length);
        for (int i = 0; i < c.length; i++)
        {
            list.add(c[i]);
        }
        return Collections.unmodifiableCollection(list);
    }


    public static Collection<Long> fromLongArray(final long[] c)
    {
        if (null == c || c.length <= 0)
        {
            return Collections.emptyList();
        }
        final Collection<Long> list = new ArrayList<>(c.length);
        for (int i = 0; i < c.length; i++)
        {
            list.add(c[i]);
        }
        return Collections.unmodifiableCollection(list);
    }


    public static Collection<Short> fromShortArray(final short[] c)
    {
        if (null == c || c.length <= 0)
        {
            return Collections.emptyList();
        }
        final Collection<Short> list = new ArrayList<>(c.length);
        for (int i = 0; i < c.length; i++)
        {
            list.add(c[i]);
        }
        return Collections.unmodifiableCollection(list);
    }


    public static Collection<Double> fromDoubleArray(final double[] c)
    {
        if (null == c || c.length <= 0)
        {
            return Collections.emptyList();
        }
        final Collection<Double> list = new ArrayList<>(c.length);
        for (int i = 0; i < c.length; i++)
        {
            list.add(c[i]);
        }
        return Collections.unmodifiableCollection(list);
    }


    public static Collection<Float> fromFloatArray(final float[] c)
    {
        if (null == c || c.length <= 0)
        {
            return Collections.emptyList();
        }
        final Collection<Float> list = new ArrayList<>(c.length);
        for (int i = 0; i < c.length; i++)
        {
            list.add(c[i]);
        }
        return Collections.unmodifiableCollection(list);
    }


    public static Collection<Character> fromCharArray(final char[] c)
    {
        if (null == c || c.length <= 0)
        {
            return Collections.emptyList();
        }
        final Collection<Character> list = new ArrayList<>(c.length);
        for (int i = 0; i < c.length; i++)
        {
            list.add(c[i]);
        }
        return Collections.unmodifiableCollection(list);
    }


    public static Collection<Boolean> fromBooleanArray(final boolean[] c)
    {
        if (null == c || c.length <= 0)
        {
            return Collections.emptyList();
        }
        final Collection<Boolean> list = new ArrayList<>(c.length);
        for (int i = 0; i < c.length; i++)
        {
            list.add(c[i]);
        }
        return Collections.unmodifiableCollection(list);
    }


    public static int[] toIntArray(final Collection<Integer> c)
    {
        if (null == c || c.isEmpty())
        {
            return new int[0];
        }
        final int[] list = new int[c.size()];
        int i = 0;
        for (final Number item : c)
        {
            list[i] = item.intValue();
            i++;
        }
        return list;
    }


    public static long[] toLongArray(final Collection<Long> c)
    {
        if (null == c || c.isEmpty())
        {
            return new long[0];
        }
        final long[] list = new long[c.size()];
        int i = 0;
        for (final Number item : c)
        {
            list[i] = item.longValue();
            i++;
        }
        return list;
    }


    public static short[] toShortArray(final Collection<Short> c)
    {
        if (null == c || c.isEmpty())
        {
            return new short[0];
        }
        final short[] list = new short[c.size()];
        int i = 0;
        for (final Number item : c)
        {
            list[i] = item.shortValue();
            i++;
        }
        return list;
    }


    public static boolean[] toBooleanArray(final Collection<Boolean> c)
    {
        if (null == c || c.isEmpty())
        {
            return new boolean[0];
        }
        final boolean[] list = new boolean[c.size()];
        int i = 0;
        for (final Boolean item : c)
        {
            list[i] = item;
            i++;
        }
        return list;
    }


    public static float[] toFloatArray(final Collection<Float> c)
    {
        if (null == c || c.isEmpty())
        {
            return new float[0];
        }
        final float[] list = new float[c.size()];
        int i = 0;
        for (final Number item : c)
        {
            list[i] = item.floatValue();
            i++;
        }
        return list;
    }


    public static double[] toDoubleArray(final Collection<Double> c)
    {
        if (null == c || c.isEmpty())
        {
            return new double[0];
        }
        final double[] list = new double[c.size()];
        int i = 0;
        for (final Number item : c)
        {
            list[i] = item.doubleValue();
            i++;
        }
        return list;
    }


    public static char[] toCharArray(final Collection<Character> c)
    {
        if (null == c || c.isEmpty())
        {
            return new char[0];
        }
        final char[] list = new char[c.size()];
        int i = 0;
        for (final Character item : c)
        {
            list[i] = item;
            i++;
        }
        return list;
    }


    private DataUtils()
    {

    }
}
