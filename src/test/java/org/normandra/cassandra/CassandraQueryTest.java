package org.normandra.cassandra;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.normandra.DatabaseQuery;
import org.normandra.EntityManager;
import org.normandra.EntityManagerFactory;
import org.normandra.entities.AnimalEntity;
import org.normandra.entities.CatEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.ZooEntity;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * test query parser
 * <p>
 * User: bowen
 * Date: 4/5/14
 */
public class CassandraQueryTest extends BaseCassandraTest
{
    private EntityManager manager;

    private EntityManagerFactory factory;


    @Before
    public void create() throws Exception
    {
        this.factory = new EntityManagerFactory.Builder()
                .withClass(CatEntity.class)
                .withClass(DogEntity.class)
                .withClass(ZooEntity.class)
                .withParameter(CassandraDatabase.HOSTS, "localhost")
                .withParameter(CassandraDatabase.PORT, port)
                .withParameter(CassandraDatabase.KEYSPACE, keyspace)
                .withDatabaseConstruction(construction)
                .create();
        this.factory.registerQuery(DogEntity.class, "dog_by_id", "select from DogEntity where id = :id");
        this.manager = this.factory.create();
    }


    @After
    public void destroy() throws IOException
    {
        if (this.manager != null)
        {
            this.manager.close();
            this.manager = null;
        }
        CassandraUtil.reset();
    }


    @Test
    public void testSimple() throws Exception
    {
        final DogEntity dog = new DogEntity("sophi", 2);
        this.manager.save(dog);
        Assert.assertNotNull(dog.getId());
        this.manager.clear();

        final Map<String, Object> params = new TreeMap<>();
        params.put("id", dog.getId());

        final DatabaseQuery<DogEntity> queryByTable = this.manager.query(DogEntity.class, "dog_by_id", params);
        Assert.assertNotNull(queryByTable);
        Collection<?> elements = queryByTable.list();
        Assert.assertNotNull(queryByTable);
        Assert.assertEquals(1, elements.size());

        final DatabaseQuery<AnimalEntity> queryNamed = this.manager.query(AnimalEntity.class, "Animal.findByID", params);
        Assert.assertNotNull(queryByTable);
        elements = queryByTable.list();
        Assert.assertNotNull(queryByTable);
        Assert.assertEquals(1, elements.size());
    }
}