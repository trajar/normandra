package org.normandra.meta;

import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

/**
 * annotation parser unit tests
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class AnnotationParserTest
{
    @Test
    public void testSimple()
    {
        final AnnotationParser parser = new AnnotationParser(SimpleEntity.class);
        Assert.assertTrue(parser.isEntity(SimpleEntity.class));
        Assert.assertEquals("SimpleEntity", parser.getEntity(SimpleEntity.class));
        Assert.assertEquals("simple_entity", parser.getTable(SimpleEntity.class));
        final List<ColumnMeta> columns = parser.getColumns(SimpleEntity.class);
        Assert.assertFalse(columns.isEmpty());
        Assert.assertEquals(3, columns.size());
    }


    @Test
    public void testInheritance()
    {
        AnnotationParser parser = new AnnotationParser(CatEntity.class);
        Assert.assertTrue(parser.isEntity(CatEntity.class));
        Assert.assertEquals("CatEntity", parser.getEntity(CatEntity.class));
        Assert.assertEquals("animal", parser.getTable(CatEntity.class));
        EntityMeta meta = parser.read().iterator().next();
        Assert.assertTrue(meta.hasColumn("type"));
    }

}
