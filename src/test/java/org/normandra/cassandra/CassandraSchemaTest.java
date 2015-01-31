package org.normandra.cassandra;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.normandra.data.BasicColumnAccessorFactory;
import org.normandra.data.CompositeIdAccessor;
import org.normandra.data.NullIdAccessor;
import org.normandra.entities.CatEntity;
import org.normandra.entities.ClassEntity;
import org.normandra.entities.CompositeIndexEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.SimpleEntity;
import org.normandra.entities.StudentDirectoryEntity;
import org.normandra.entities.StudentEntity;
import org.normandra.entities.StudentIndexEntity;
import org.normandra.entities.ZooEntity;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 * cassandra unit tests
 * <p/>
 * User: bowen
 * Date: 9/7/13
 */
public class CassandraSchemaTest
{
    private final CassandraTestHelper helper = new CassandraTestHelper();


    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraTestHelper.setup();
    }


    @Before
    public void create() throws Exception
    {
        helper.create(Arrays.asList(DogEntity.class, CatEntity.class, ZooEntity.class));
    }


    @After
    public void destroy() throws Exception
    {
        helper.destroy();
    }


    @Test
    public void testSimple() throws Exception
    {
        // we should start with clean keyspace
        final CassandraDatabase database = helper.getDatabase();
        final AnnotationParser parser = new AnnotationParser(new BasicColumnAccessorFactory(), SimpleEntity.class);
        final EntityMeta entity = parser.read().iterator().next();
        Assert.assertNotNull(entity);
        for (final TableMeta table : entity.getTables())
        {
            Assert.assertFalse(database.hasTable(table.getName()));
        }

        // construct schema
        final DatabaseMeta meta = new DatabaseMeta(Arrays.asList(entity));
        database.refresh(meta);
        final String table = meta.getTables().iterator().next();
        Assert.assertTrue(database.hasTable(table));
        Assert.assertTrue(database.hasTable(table));
        Assert.assertTrue(database.hasColumn(table, "id"));
        Assert.assertFalse(database.hasColumn(table, "name"));
        Assert.assertTrue(database.hasColumn(table, "name_column"));
        Assert.assertTrue(database.hasColumn(table, "values"));
        Assert.assertFalse(database.hasColumn(table, "foo"));

        // refresh without error
        database.refresh(meta);
        Assert.assertTrue(database.hasTable(table));
        Assert.assertTrue(database.hasColumn(table, "id"));
        Assert.assertTrue(database.hasColumn(table, "name_column"));
        Assert.assertTrue(database.hasColumn(table, "values"));
    }


    @Test
    public void testInheritance() throws Exception
    {
        // build meta-data for all entities
        final CassandraDatabase database = helper.getDatabase();
        final Set<EntityMeta> list = new ArraySet<>();
        for (final Class<?> clazz : Arrays.asList(CatEntity.class, DogEntity.class))
        {
            final AnnotationParser parser = new AnnotationParser(new BasicColumnAccessorFactory(), clazz);
            list.addAll(parser.read());
        }

        // construct schema
        final DatabaseMeta meta = new DatabaseMeta(list);
        database.refresh(meta);
        Assert.assertTrue(database.hasTable("animal"));
        Assert.assertTrue(database.hasColumn("animal", "id"));
        Assert.assertTrue(database.hasColumn("animal", "type"));
        Assert.assertFalse(database.hasColumn("animal", "numBarks"));
        Assert.assertTrue(database.hasColumn("animal", "num_barks"));
        Assert.assertTrue(database.hasColumn("animal", "litter_box"));
    }


    @Test
    public void testComposite() throws Exception
    {
        final AnnotationParser parser = new AnnotationParser(new BasicColumnAccessorFactory(), StudentIndexEntity.class, CompositeIndexEntity.class);
        final Collection<EntityMeta> list = parser.read();
        Assert.assertFalse(list.isEmpty());
        Assert.assertEquals(2, list.size());

        final CassandraDatabase database = helper.getDatabase();
        final DatabaseMeta meta = new DatabaseMeta(list);
        database.refresh(meta);
        Assert.assertTrue(database.hasTable("student_index"));
        Assert.assertTrue(database.hasColumn("student_index", "name"));
        Assert.assertTrue(database.hasColumn("student_index", "classroom_id"));
        Assert.assertTrue(database.hasTable("composite_index"));
        Assert.assertFalse(database.hasColumn("composite_index", "key"));
        Assert.assertTrue(database.hasColumn("composite_index", "id"));
        Assert.assertTrue(database.hasColumn("composite_index", "name"));

        final EntityMeta student = meta.getEntity("student_index");
        final TableMeta studentTable = student.getTables().iterator().next();
        Assert.assertNotNull(student);
        Assert.assertEquals(2, studentTable.getPrimaryKeys().size());
        Assert.assertNotNull(student.getId());
        Assert.assertTrue(student.getId() instanceof NullIdAccessor);

        final EntityMeta composite = meta.getEntity("composite_index");
        final TableMeta compositeTable = student.getTables().iterator().next();
        Assert.assertNotNull(composite);
        Assert.assertEquals(2, compositeTable.getPrimaryKeys().size());
        Assert.assertNotNull(composite.getId());
        Assert.assertTrue(composite.getId() instanceof CompositeIdAccessor);
    }


    @Test
    public void testJoinTable() throws Exception
    {
        final AnnotationParser parser = new AnnotationParser(new BasicColumnAccessorFactory(), StudentEntity.class, StudentDirectoryEntity.class, ClassEntity.class);
        final Collection<EntityMeta> list = parser.read();
        Assert.assertFalse(list.isEmpty());
        Assert.assertEquals(3, list.size());

        final CassandraDatabase database = helper.getDatabase();
        final DatabaseMeta meta = new DatabaseMeta(list);
        database.refresh(meta);

        Assert.assertTrue(meta.getTables().contains("classroom"));
        Assert.assertTrue(meta.getTables().contains("student_directory_xref"));

        final EntityMeta classroomMeta = meta.getEntity("classroom");
        Assert.assertNotNull(classroomMeta);
        Assert.assertNotNull(classroomMeta.getTable("classroom"));

        final EntityMeta direcoryMeta = meta.getEntity("student_directory");
        final TableMeta joinMeta = direcoryMeta.getTable("student_directory_xref");
        Assert.assertTrue(joinMeta.isJoinTable());
        Assert.assertTrue(joinMeta.hasColumn("id"));
        Assert.assertTrue(joinMeta.hasColumn("student_id"));
        Assert.assertEquals(2, joinMeta.getColumns().size());
        Assert.assertEquals(2, joinMeta.getPrimaryKeys().size());
    }
}
