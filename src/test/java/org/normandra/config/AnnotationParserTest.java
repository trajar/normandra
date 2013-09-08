package org.normandra.config;

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
        Assert.assertTrue(parser.isEntity());
        Assert.assertEquals("SimpleEntity", parser.getEntity());
        Assert.assertEquals("simple_entity", parser.getTable());
        final List<ColumnMeta> columns = parser.getColumns();
        Assert.assertFalse(columns.isEmpty());
        Assert.assertEquals(3, columns.size());
    }


    @Test
    public void testInheritance()
    {
        AnnotationParser parser = new AnnotationParser(CatEntity.class);
        Assert.assertTrue(parser.isEntity());
        Assert.assertEquals("CatEntity", parser.getEntity());
        Assert.assertEquals("animal", parser.getTable());
        EntityMeta meta = parser.readEntity();
        Assert.assertTrue(meta.hasColumn("type"));
    }

}
