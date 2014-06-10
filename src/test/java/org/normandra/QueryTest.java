package org.normandra;

import junit.framework.Assert;
import org.junit.Test;
import org.normandra.entities.AnimalEntity;
import org.normandra.entities.DogEntity;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * test query parser
 * <p/>
 * User: bowen
 * Date: 4/5/14
 */
public class QueryTest extends BaseTest
{
    @Test
    public void testSimple() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            final EntityManager manager = helper.getManager();

            final DogEntity dog = new DogEntity("sophi", 2);
            manager.save(dog);
            Assert.assertNotNull(dog.getId());
            manager.clear();

            final Map<String, Object> params = new TreeMap<>();
            params.put("id", dog.getId());

            final DatabaseQuery<DogEntity> queryByTable = manager.query(DogEntity.class, "dog_by_id", params);
            Assert.assertNotNull(queryByTable);
            Collection<?> elements = queryByTable.list();
            Assert.assertNotNull(queryByTable);
            Assert.assertEquals(1, elements.size());

            final DatabaseQuery<AnimalEntity> queryNamed = manager.query(AnimalEntity.class, "Animal.findByID", params);
            Assert.assertNotNull(queryNamed);
            elements = queryNamed.list();
            Assert.assertNotNull(queryNamed);
            Assert.assertEquals(1, elements.size());
        }
    }
}