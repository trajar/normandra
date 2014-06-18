package org.normandra;

import java.util.Collection;

/**
 * a test helper framework api
 * <p/>
 * User: bowen
 * Date: 6/8/14
 */
public interface TestHelper
{
    Database getDatabase();
    DatabaseSession getSession();
    EntityManager getManager();
    void create(Collection<Class> types) throws Exception;
    void destroy() throws Exception;
}
