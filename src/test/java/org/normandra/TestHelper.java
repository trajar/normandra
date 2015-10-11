package org.normandra;

import java.util.Collection;

/**
 * a test helper framework api
 * <p/>
 * 
 * Date: 6/8/14
 */
public interface TestHelper
{
    Database getDatabase();
    DatabaseSession getSession();
    EntityManager getManager();
    EntityManagerFactory getFactory();
    void create(Collection<Class> types) throws Exception;
    void cleanup();
}
