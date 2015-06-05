package org.normandra.association;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.normandra.EntitySession;
import org.normandra.meta.EntityMeta;
import org.normandra.util.CompositeClassLoader;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * methods for working with jpa associations
 * <p>
 * User: bowen
 * Date: 2/2/14
 */
public class AssociationUtils
{
    private final static Map<Class<?>, Class<?>> proxies = new ConcurrentHashMap<>();


    public static boolean isLoaded(final Object element)
    {
        if (!isProxy(element))
        {
            throw new IllegalStateException("Instance [" + element + "] is not a proxy element.");
        }
        final ProxyObject proxy = (ProxyObject) element;
        final LazyAssociationHandler handler = (LazyAssociationHandler) proxy.getHandler();
        return handler.isLoaded();
    }


    public static boolean isProxy(final Object element)
    {
        if (null == element)
        {
            return false;
        }
        if (element instanceof ProxyObject)
        {
            final MethodHandler methodHandler = ((ProxyObject) element).getHandler();
            if (methodHandler instanceof LazyAssociationHandler)
            {
                return true;
            }
        }
        return false;
    }


    public static Object createProxy(final EntityMeta meta, final Object key, final EntitySession session, final ElementIdentity factory) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        if (null == meta)
        {
            return null;
        }

        final Class<?> clazz = meta.getType();
        Class<?> proxy = proxies.get(clazz);
        if (null == proxy)
        {
            ProxyFactory.classLoaderProvider = new ProxyFactory.ClassLoaderProvider()
            {
                @Override
                public ClassLoader get(final ProxyFactory pf)
                {
                    final List<ClassLoader> loaders = new ArrayList<>(2);
                    loaders.add(AssociationUtils.class.getClassLoader());
                    if (null == pf)
                    {
                        return new CompositeClassLoader(ProxyFactory.class.getClassLoader(), loaders);
                    }
                    final Class<?> type = pf.getSuperclass();
                    if (type != null)
                    {
                        loaders.add(type.getClassLoader());
                    }
                    return new CompositeClassLoader(ProxyFactory.class.getClassLoader(), loaders);
                }
            };
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setSuperclass(clazz);
            proxyFactory.setUseCache(false);
            proxy = proxyFactory.createClass();
            proxies.put(clazz, proxy);
        }

        final Object instance = proxy.newInstance();
        if (null == instance)
        {
            return null;
        }

        final AssociationAccessor accessor = new ManyToOneAccessor(key, session, factory);
        final LazyAssociationHandler handler = new LazyAssociationHandler(meta, accessor, session);
        ((ProxyObject) instance).setHandler(handler);
        return clazz.cast(instance);
    }


    private AssociationUtils()
    {

    }
}
