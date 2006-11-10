/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.util;

import java.io.*;


/**
 * Miscellaneous static utilities that don't fit into other categories.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoUtil
{
    
    //~ Methods ----------------------------------------------------------------

    /**
     * Calculates the memory used by a string's data (not including the String
     * object itself). This represents the actual memory used by the Java
     * Unicode representation, not an encoding.
     *
     * @return number of bytes used
     */
    public static int getStringMemoryUsage(String s)
    {
        return s.length() * 2;
    }
    
    /**
     * Estimates the memory used by the Fennel portion of a query plan by
     * taking the memory used by the XMI representation of the plan and
     * multiplying by a constant factor.  That constant factor is estimated as
     * 2, based on measurements correlating between the the XMI plan string
     * length and the actual Fennel memory used to construct the Fennel stream
     * graphs.  The actual measured value was between .6 and .95 of the XMI
     * plan size.  Therefore, we use 1 as a conservative estimate.  But since
     * we've already accounted for half of the XMI plan memory in the cache
     * entry associated with the SQL statement, we reduce by .5 to arrive at
     * 1.5.
     * 
     * @param s XMI string
     * 
     * @return estimated memory usage
     */
    public static long getFennelMemoryUsage(String s)
    {
        int xmiSize = FarragoUtil.getStringMemoryUsage(s);
        return (long) ((double) xmiSize * 1.5);
    }

    /**
     * Copies everything from a Reader into a Writer.
     *
     * @param reader source
     * @param writer destination
     *
     * @return number of chars copied
     */
    public static int copyFromReaderToWriter(
        Reader reader,
        Writer writer)
        throws IOException
    {
        char [] buf = new char[4096];
        int charsCopied = 0;
        for (;;) {
            int charsRead = reader.read(buf, 0, buf.length);
            if (charsRead == -1) {
                return charsCopied;
            }
            writer.write(buf, 0, charsRead);
            charsCopied += charsRead;
        }
    }

    
    public static String exceptionToString(final Throwable ex)
    {
        exceptionToString(ex, "; ");
    }
    
    /**
     * Converts any Throwable and its causes to a String.
     *
     * @param ex Throwable to be converted
     * @param sep String the stack line separator
     * @return ex as a String
     */

    public static String exceptionToString(final Throwable ex, String sep)
    {
        if(sep == null)
        {
            sep = "; ";
        }

        String result = null;
        if (ex != null) {
            Throwable t = ex;
            StringBuilder sb = new StringBuilder();
            while (t != null) {
                sb.append(t.getClass().getName());
                sb.append(":  ");
                sb.append(t.getLocalizedMessage());
                t = t.getCause();
                if (t != null) {
                    sb.append(sep);
                }
            }
            result = sb.toString();
        }
        return result;
    }

}

// End FarragoUtil.java
