package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.apache.commons.io.FileUtils;
import org.normandra.DatabaseConstruction;
import org.normandra.EntityManager;
import org.normandra.EntityManagerFactory;
import org.normandra.TestHelper;
import org.normandra.cache.EntityCacheFactory;
import org.normandra.cache.MapFactory;
import org.normandra.cache.MemoryCache;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * orientdb test utilities
 * <p>
 * Date: 6/8/14
 */
public class OrientTestHelper implements TestHelper
{
    public static final DatabaseConstruction construction = DatabaseConstruction.RECREATE;

    private static final File dir = new File("target/embeddedOrientDB");

    public static final String path = "plocal:" + dir.getPath();

    public static final String user = "admin";

    public static final String password = "admin";

    private final Set<Class> types = new HashSet<>();

    private EntityCacheFactory cache = new MemoryCache.Factory(MapFactory.withConcurrency());

    private OrientDatabase database;

    private OrientDatabaseSession session;

    private EntityManagerFactory factory;

    private EntityManager manager;

    public static void setup() throws Exception
    {
        System.setProperty("log.console.level", "SEVERE");
        if (dir.exists())
        {
            FileUtils.forceDelete(dir);
        }
        dir.mkdirs();
        try (final ODatabase db = new ODatabaseDocumentTx(path))
        {
            db.setProperty("storage.keepOpen", Boolean.FALSE);
            db.create();
        }
        Thread.sleep(100);
    }

    @Override
    public OrientDatabase getDatabase()
    {
        if (null == this.database)
        {
            this.createDatabase();
        }
        return this.database;
    }

    @Override
    public OrientDatabaseSession getSession()
    {
        if (null == this.session)
        {
            this.createDatabase();
        }
        return this.session;
    }

    private void createDatabase()
    {
        this.database = new OrientDatabaseFactory(path, user, password, cache, construction).create();
        this.session = this.database.createSession();
    }

    @Override
    public EntityManagerFactory getFactory()
    {
        if (null == this.factory)
        {
            this.createManager();
        }
        return this.factory;
    }

    @Override
    public EntityManager getManager()
    {
        if (null == this.manager)
        {
            this.createManager();
        }
        return this.manager;
    }

    private void createManager()
    {
        try
        {
            final EntityManagerFactory.Builder builder = new EntityManagerFactory.Builder()
                .withType(EntityManagerFactory.Type.ORIENTDB)
                .withParameter(OrientDatabase.URL, path)
                .withParameter(OrientDatabase.USER_ID, user)
                .withParameter(OrientDatabase.PASSWORD, password)
                .withDatabaseConstruction(construction);
            for (final Class<?> clazz : types)
            {
                builder.withClass(clazz);
            }
            this.factory = builder.create();
            this.manager = this.factory.create();
        }
        catch (final Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void create(final Collection<Class> c) throws Exception
    {
        this.types.clear();
        this.types.addAll(c);
    }

    @Override
    public void cleanup()
    {
        try
        {
            if (this.session != null)
            {
                this.session.close();
                this.session = null;
            }
            if (this.manager != null)
            {
                this.manager.close();
                this.manager = null;
            }
            if (this.database != null)
            {
                this.database.close();
                this.database.shutdown();
                this.database = null;
            }
        }
        catch (final Exception e)
        {
//          e.printStackTrace();
        }
    }
}
