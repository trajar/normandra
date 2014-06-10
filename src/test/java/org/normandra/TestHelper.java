package org.normandra;

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
    void create() throws Exception;
    void destroy() throws Exception;
}
