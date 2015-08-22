package org.normandra.orientdb;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseConstruction;
import org.normandra.DatabaseFactory;
import org.normandra.cache.EntityCacheFactory;

/**
 * a database factory
 * <p>
 * User: bowen Date: 5/14/14
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


    @Override
    public OrientDatabase create()
    {
        return new OrientDatabase(this.url, this.userId, this.password, this.cache, this.constructionMode);
    }
}
