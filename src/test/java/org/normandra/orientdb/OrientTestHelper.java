package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.normandra.DatabaseConstruction;
import org.normandra.EntityManager;
import org.normandra.EntityManagerFactory;
import org.normandra.TestHelper;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * orientdb test utilities
 * <p>
 * User: bowen
 * Date: 6/8/14
 */
public class OrientTestHelper implements TestHelper
{
    public static final DatabaseConstruction construction = DatabaseConstruction.RECREATE;

    public static final File dir = new File("target/embeddedOrientDB");

    public static final String path = "plocal:" + dir.getPath();

    public static final String user = "admin";

    public static final String password = "admin";

    private OrientDatabase database;

    private OrientDatabaseSession session;

    private EntityManager manager;


    public static void setup() throws Exception
    {
        // storage.keepOpen=false
        BasicConfigurator.configure();
        if (dir.exists())
        {
            FileUtils.deleteDirectory(dir);
        }
        final ODatabase db = new ODatabaseDocumentTx(path).create();
        db.setProperty("storage.keepOpen", Boolean.FALSE);
        db.close();
    }


    @Override
    public OrientDatabase getDatabase()
    {
        return this.database;
    }


    @Override
    public OrientDatabaseSession getSession()
    {
        return this.session;
    }


    public EntityManager getManager()
    {
        return this.manager;
    }


    @Override
    public void create(final Collection<Class> types) throws Exception
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
        this.manager = builder.create().create();
        this.database = new OrientDatabaseFactory(path, user, password, construction).create();
        this.session = this.database.createSession();
    }


    @Override
    public void destroy() throws IOException
    {
        if (this.session != null)
        {
            this.session.close();
            this.session = null;
        }
        if (this.database != null)
        {
            this.database.close();
            this.database = null;
        }
        if (this.manager != null)
        {
            this.manager.close();
            this.manager = null;
        }
    }
}
