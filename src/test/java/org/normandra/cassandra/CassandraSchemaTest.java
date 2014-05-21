package org.normandra.cassandra;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.normandra.data.CompositeIdAccessor;
import org.normandra.data.NullIdAccessor;
import org.normandra.entities.CatEntity;
import org.normandra.entities.ClassEntity;
import org.normandra.entities.CompositeIndexEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.SimpleEntity;
import org.normandra.entities.StudentEntity;
import org.normandra.entities.StudentIndexEntity;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * cassandra unit tests
 * <p>
 * User: bowen
 * Date: 9/7/13
 */
public class CassandraSchemaTest extends BaseCassandraTest
{
    private CassandraDatabase database;


    @Before
    public void create() throws Exception
    {
        this.database = new CassandraDatabaseFactory(keyspace, "localhost", port, construction).create();
    }


    @After
    public void destroy() throws IOException
    {
        if (this.database != null)
        {
            this.database.close();
            this.database = null;
        }
        CassandraTestUtil.reset();
    }


    @Test
    public void testSimple() throws Exception
    {
        // we should start with clean keyspace
        final AnnotationParser parser = new AnnotationParser(SimpleEntity.class);
        final EntityMeta entity = parser.read().iterator().next();
        Assert.assertNotNull(entity);
        for (final TableMeta table : entity.getTables())
        {
            Assert.assertFalse(this.database.hasTable(table.getName()));
        }

        // construct schema
        final DatabaseMeta meta = new DatabaseMeta(Arrays.asList(entity));
        this.database.refresh(meta);
        final String table = meta.getTables().iterator().next();
        Assert.assertTrue(this.database.hasTable(table));
        Assert.assertTrue(this.database.hasTable(table));
        Assert.assertTrue(this.database.hasColumn(table, "id"));
        Assert.assertFalse(this.database.hasColumn(table, "name"));
        Assert.assertTrue(this.database.hasColumn(table, "name_column"));
        Assert.assertTrue(this.database.hasColumn(table, "values"));
        Assert.assertFalse(this.database.hasColumn(table, "foo"));

        // refresh without error
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasTable(table));
        Assert.assertTrue(this.database.hasColumn(table, "id"));
        Assert.assertTrue(this.database.hasColumn(table, "name_column"));
        Assert.assertTrue(this.database.hasColumn(table, "values"));
    }


    @Test
    public void testInheritance() throws Exception
    {
        // build meta-data for all entities
        final List<EntityMeta> list = new ArrayList<>();
        for (final Class<?> clazz : Arrays.asList(CatEntity.class, DogEntity.class))
        {
            final AnnotationParser parser = new AnnotationParser(clazz);
            final EntityMeta entity = parser.read().iterator().next();
            Assert.assertNotNull(entity);
            for (final TableMeta table : entity.getTables())
            {
                Assert.assertFalse(this.database.hasTable(table.getName()));
            }
            list.add(entity);
        }

        // construct schema
        final DatabaseMeta meta = new DatabaseMeta(list);
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasTable("animal"));
        Assert.assertTrue(this.database.hasColumn("animal", "id"));
        Assert.assertTrue(this.database.hasColumn("animal", "type"));
        Assert.assertFalse(this.database.hasColumn("animal", "numBarks"));
        Assert.assertTrue(this.database.hasColumn("animal", "num_barks"));
        Assert.assertTrue(this.database.hasColumn("animal", "litter_box"));
    }


    @Test
    public void testComposite() throws Exception
    {
        final AnnotationParser parser = new AnnotationParser(StudentIndexEntity.class, CompositeIndexEntity.class);
        final Collection<EntityMeta> list = parser.read();
        Assert.assertFalse(list.isEmpty());
        Assert.assertEquals(2, list.size());

        final DatabaseMeta meta = new DatabaseMeta(list);
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasTable("student_index"));
        Assert.assertTrue(this.database.hasColumn("student_index", "name"));
        Assert.assertTrue(this.database.hasColumn("student_index", "classroom_id"));
        Assert.assertTrue(this.database.hasTable("composite_index"));
        Assert.assertFalse(this.database.hasColumn("composite_index", "key"));
        Assert.assertTrue(this.database.hasColumn("composite_index", "id"));
        Assert.assertTrue(this.database.hasColumn("composite_index", "name"));

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
        final AnnotationParser parser = new AnnotationParser(StudentEntity.class, ClassEntity.class);
        final Collection<EntityMeta> list = parser.read();
        Assert.assertFalse(list.isEmpty());
        Assert.assertEquals(2, list.size());

        final DatabaseMeta meta = new DatabaseMeta(list);
        this.database.refresh(meta);

        Assert.assertTrue(meta.getTables().contains("classroom"));
        Assert.assertTrue(meta.getTables().contains("classroom_student_xref"));

        final EntityMeta classroomMeta = meta.getEntity("classroom");
        Assert.assertNotNull(classroomMeta);
        Assert.assertNotNull(classroomMeta.getTable("classroom"));
        Assert.assertNotNull(classroomMeta.getTable("classroom_student_xref"));

        final TableMeta joinMeta = classroomMeta.getTable("classroom_student_xref");
        Assert.assertTrue(joinMeta.isSecondary());
        Assert.assertTrue(joinMeta.hasColumn("id"));
        Assert.assertTrue(joinMeta.hasColumn("student_id"));
        Assert.assertEquals(2, joinMeta.getColumns().size());
        Assert.assertEquals(2, joinMeta.getPrimaryKeys().size());
    }
}
