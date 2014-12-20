package org.normandra.cache;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * a generic factory used to create map instances
 * <p>
 * User: bowen Date: 7/14/14
 */
public interface MapFactory
{
    public static MapFactory withWeakKeys()
    {
        return new MapFactory()
        {
            @Override
            public Map create()
            {
                return new WeakHashMap();
            }
        };
    }

    public static MapFactory withConcurrency()
    {
        return new MapFactory()
        {
            @Override
            public Map create()
            {
                return new ConcurrentHashMap<>();
            }
        };
    }

    Map create();
}
