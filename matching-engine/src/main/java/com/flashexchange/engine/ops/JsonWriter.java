package com.flashexchange.engine.ops;

import java.util.Collection;
import java.util.Map;

/**
 * Minimal JSON serialiser for operational endpoints (maps, collections, strings, numbers, booleans, null).
 * Kept dependency free on purpose; not used on the matching hot path.
 */
public final class JsonWriter
{
    private JsonWriter()
    {
    }

    /**
     * Serialise a value.
     *
     * @param value map, collection, string, number, boolean or {@code null}.
     * @return JSON text.
     */
    public static String toJson(final Object value)
    {
        final StringBuilder sb = new StringBuilder(256);
        write(sb, value);
        return sb.toString();
    }

    private static void write(final StringBuilder sb, final Object value)
    {
        if (value == null)
        {
            sb.append("null");
        }
        else if (value instanceof Map<?, ?> map)
        {
            sb.append('{');
            boolean first = true;
            for (final Map.Entry<?, ?> e : map.entrySet())
            {
                if (!first)
                {
                    sb.append(',');
                }
                first = false;
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(':');
                write(sb, e.getValue());
            }
            sb.append('}');
        }
        else if (value instanceof Collection<?> collection)
        {
            sb.append('[');
            boolean first = true;
            for (final Object item : collection)
            {
                if (!first)
                {
                    sb.append(',');
                }
                first = false;
                write(sb, item);
            }
            sb.append(']');
        }
        else if (value instanceof Number || value instanceof Boolean)
        {
            sb.append(value);
        }
        else
        {
            writeString(sb, value.toString());
        }
    }

    private static void writeString(final StringBuilder sb, final String s)
    {
        sb.append('"');
        for (int i = 0; i < s.length(); i++)
        {
            final char c = s.charAt(i);
            switch (c)
            {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default ->
                {
                    if (c < 0x20)
                    {
                        sb.append(String.format("\\u%04x", (int)c));
                    }
                    else
                    {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
