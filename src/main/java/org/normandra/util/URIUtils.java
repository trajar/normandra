package org.normandra.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.regex.Pattern;

/**
 * set of common URI/URL utility methods
 * <p>
 * 
 * Date: 9/1/14
 */
public class URIUtils
{
    private static final Logger logger = LoggerFactory.getLogger(URIUtils.class);

    private static Pattern protocol = Pattern.compile("^\\w+:");


    public static URI createURI(final String path)
    {
        if (null == path || path.isEmpty())
        {
            return null;
        }

        if (protocol.matcher(path).matches())
        {
            try
            {
                return new URI(path);
            }
            catch (final URISyntaxException e)
            {
                logger.info("Unable to parse URI from [" + path + "].", e);
            }
        }

        try
        {
            return new File(path).toURI();
        }
        catch (final Exception e)
        {
            logger.info("Unable to build file path [" + path + "] into URI.", e);
        }

        return null;
    }


    public static URL createURL(final String path)
    {
        if (null == path || path.isEmpty())
        {
            return null;
        }

        if (protocol.matcher(path).matches())
        {
            try
            {
                return new URL(path);
            }
            catch (final MalformedURLException e)
            {
                logger.info("Unable to parse URI from [" + path + "].", e);
            }
        }

        try
        {
            return new File(path).toURI().toURL();
        }
        catch (final Exception e)
        {
            logger.info("Unable to build file path [" + path + "] into URI.", e);
        }

        return null;
    }


    private URIUtils()
    {
    }
}
