package org.normandra.association;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.normandra.DatabaseSession;
import org.normandra.meta.EntityMeta;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * methods for working with jpa associations
 * <p/>
 * User: bowen
 * Date: 2/2/14
 */
public class AssociationUtils
{
    private final static Map<Class<?>, Class<?>> proxies = new ConcurrentHashMap<>();


    public static Object proxy(final EntityMeta meta, final Object key, final DatabaseSession session) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        if (null == meta)
        {
            return null;
        }

        final Class<?> clazz = meta.getType();
        Class<?> proxy = proxies.get(clazz);
        if (null == proxy)
        {
            final ProxyFactory factory = new ProxyFactory();
            factory.setSuperclass(clazz);
            factory.setUseCache(false);
            proxy = factory.createClass();
            proxies.put(clazz, proxy);
        }

        final Object instance = proxy.newInstance();
        if (null == instance)
        {
            return null;
        }

        final AssociationAccessor accessor = new ManyToOneAccessor(meta, key, session);
        final MethodHandler handler = new LazyAssociationHandler(meta, accessor, session);
        ((ProxyObject) instance).setHandler(handler);
        return clazz.cast(instance);
    }


    private AssociationUtils()
    {

    }
}
