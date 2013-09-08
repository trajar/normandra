package org.normandra.util;

/**
 * some simple string functions
 */
public class CaseUtils
{
    /**
     * converts ruby to camel case
     */
    public static String snakeToCamelCase(final String rubyCase)
    {
        return camelCase(rubyCase, "_");
    }


    /**
     * converts ruby to camel case
     */
    public static String camelToSnakeCase(final String camelCase)
    {
        // check for null
        if (camelCase == null || camelCase.isEmpty())
        {
            return "";
        }

        // simple regex
        return camelCase.replaceAll("\\B([A-Z])", "_$1").toLowerCase();
    }


    /**
     * converts string to ruby case, using the specified string delimeter
     */
    public static String snakeCase(final String value, final String delim)
    {
        // check for null
        if (value == null || value.isEmpty())
        {
            return "";
        }

        final StringBuilder result = new StringBuilder(value.length());
        final String[] parts = value.split(delim);
        if (parts != null && parts.length > 0)
        {
            boolean bMustUnderscore = false;
            for (final String part : parts)
            {
                if (part.length() > 1)
                {
                    if (bMustUnderscore)
                    {
                        result.append("_");
                    }
                    result.append(part.toLowerCase());
                    bMustUnderscore = true;
                }
                else
                {
                    result.append(part);
                }
            }
        }

        return result.toString();
    }


    /**
     * converts string to camel case, using the specified string delimeter
     */
    public static String camelCase(final String value, final String delim)
    {
        // check for null
        if (value == null || value.isEmpty())
        {
            return "";
        }

        final StringBuilder result = new StringBuilder(value.length());
        final String[] parts = value.split(delim);
        if (parts != null && parts.length > 0)
        {
            boolean mustCapitalize = false;
            for (final String part : parts)
            {
                if (part.length() > 1)
                {
                    if (mustCapitalize)
                    {
                        result.append(part.substring(0, 1).toUpperCase());
                        result.append(part.substring(1).toLowerCase());
                    }
                    else
                    {
                        result.append(part);
                        mustCapitalize = true;
                    }
                }
                else
                {
                    result.append(part);
                }
            }
        }

        return result.toString();
    }


    private CaseUtils()
    {

    }
}
