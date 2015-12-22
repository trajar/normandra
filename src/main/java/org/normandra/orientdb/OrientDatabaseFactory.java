package org.normandra.orientdb;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseConstruction;
import org.normandra.DatabaseFactory;
import org.normandra.cache.EntityCacheFactory;

/**
 * a database factory
 * <p>
 * Date: 5/14/14
 */
public class OrientDatabaseFactory implements DatabaseFactory
{
    private final String url;

    private final String userId;

    private final String password;

    private final EntityCacheFactory cache;

    private final DatabaseConstruction constructionMode;

    public OrientDatabaseFactory(final String url, final String user, final String pwd, final EntityCacheFactory cache, final DatabaseConstruction mode)
    {
        if (null == url || url.isEmpty())
        {
            throw new IllegalArgumentException("URL cannot be null/empty.");
        }
        if (null == cache)
        {
            throw new NullArgumentException("cache factory");
        }
        if (null == mode)
        {
            throw new NullArgumentException("construction mode");
        }
        this.url = url;
        this.userId = user;
        this.password = pwd;
        this.cache = cache;
        this.constructionMode = mode;
    }

    public boolean isLocal()
    {
        return this.url.toLowerCase().startsWith("plocal:") || this.url.toLowerCase().startsWith("local:");
    }

    @Override
    public OrientDatabase create()
    {
        final OrientPool pool;
        if (this.isLocal())
        {
            pool = new LocalOrientPool(this.url, this.userId, this.password);
        }
        else
        {
            pool = new FixedOrientPool(this.url, this.userId, this.password);
        }
        return new OrientDatabase(this.url, pool, this.cache, this.constructionMode);
    }
}
