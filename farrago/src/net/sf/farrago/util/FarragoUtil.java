/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.util;

import org.eigenbase.util.*;

import java.io.*;
import java.sql.*;
import java.util.logging.*;

/**
 * Miscellaneous static utilities that don't fit into other categories.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoUtil
{
    /**
     * Calculate the memory used by a string's data (not including the String
     * object itself).  This represents the actual memory used by the Java
     * Unicode representation, not an encoding.
     *
     * @return number of bytes used
     */
    public static int getStringMemoryUsage(String s)
    {
        return s.length()*2;
    }

    /**
     * Copies everything from a Reader into a Writer.
     *
     * @param reader source
     *
     * @param writer destination
     *
     * @return number of chars copied
     */
    public static int copyFromReaderToWriter(Reader reader,Writer writer)
        throws IOException
    {
        char[] buf = new char[4096];
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

    
    /**
     * Converter from any Throwable to SQLException.
     *
     * @param ex Throwable to be converted
     *
     * @param tracer Logger on which to trace exceptions as they are
     * converted
     *
     * @return ex as a SQLException
     */
    public static SQLException newSqlException(
        Throwable ex,
        Logger tracer)
    {
        tracer.severe(ex.getMessage());
        tracer.throwing("FarragoUtil","newSqlException",ex);
        
        SQLException sqlExcn;
        if (ex instanceof FarragoException) {
            // TODO:  map for SQLState
            sqlExcn = new SQLException(ex.getMessage());
        } else if (ex instanceof SQLException) {
            sqlExcn = (SQLException) ex;
        } else {
            // for anything else, include the class name
            // as part of what went wrong
            sqlExcn = new SQLException(
                ex.getClass().getName() + ": " + ex.getMessage());
        }

        // preserve additional attributes of the original excn
        sqlExcn.setStackTrace(ex.getStackTrace());

        // convert to SQLException-style chaining
        Throwable cause = ex.getCause();
        if (cause != null) {
            // NOTE jvs 18-June-2004:  reverse the order so that
            // the underlying cause comes out on top
            SQLException sqlCause = newSqlException(cause,tracer);
            sqlCause.setNextException(sqlExcn);
            return sqlCause;
        } else {
            return sqlExcn;
        }
    }
}

// End FarragoUtil.java
