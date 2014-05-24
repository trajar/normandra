package org.normandra.orientdb;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.normandra.entities.ClassEntity;
import org.normandra.entities.SimpleEntity;
import org.normandra.entities.StudentEntity;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * cassandra unit tests
 * <p/>
 * User: bowen
 * Date: 9/7/13
 */
public class OrientSchemaTest extends BaseOrientTest
{
    private OrientDatabase database;


    @Before
    public void create() throws Exception
    {
        this.database = new OrientDatabaseFactory(path, user, password, construction).create();
    }


    @After
    public void destroy() throws IOException
    {
        if (this.database != null)
        {
            this.database.close();
            this.database = null;
        }
    }


    @Test
    public void testSimple() throws Exception
    {
        // we should start with clean database
        final AnnotationParser parser = new AnnotationParser(SimpleEntity.class);
        final EntityMeta entity = parser.read().iterator().next();
        Assert.assertNotNull(entity);
        Assert.assertEquals(9, this.database.getClusters().size());
        for (final TableMeta table : entity.getTables())
        {
            Assert.assertFalse(this.database.hasCluster(table.getName()));
        }

        // construct schema
        final DatabaseMeta meta = new DatabaseMeta(Arrays.asList(entity));
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasCluster("simple_entity"));
        Assert.assertTrue(this.database.hasProperty("simple_entity", "id"));
        Assert.assertFalse(this.database.hasProperty("simple_entity", "name"));
        Assert.assertTrue(this.database.hasProperty("simple_entity", "name_column"));
        Assert.assertTrue(this.database.hasProperty("simple_entity", "values"));
        Assert.assertFalse(this.database.hasProperty("simple_entity", "foo"));

        // refresh without error
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasCluster("simple_entity"));
        Assert.assertTrue(this.database.hasProperty("simple_entity", "id"));
        Assert.assertTrue(this.database.hasProperty("simple_entity", "name_column"));
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
