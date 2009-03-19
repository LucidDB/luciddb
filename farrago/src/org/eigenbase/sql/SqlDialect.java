/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.sql;

import java.sql.*;

import java.util.*;
import java.util.regex.*;

import org.eigenbase.util.*;


/**
 * <code>SqlDialect</code> encapsulates the differences between dialects of SQL,
 * for the benefit of a {@link SqlWriter}.
 */
public class SqlDialect
{
    //~ Instance fields --------------------------------------------------------

    String databaseProductName;
    String identifierQuoteString;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>SqlDialect</code>
     *
     * @param databaseMetaData used to determine which dialect of SQL to
     * generate
     */
    public SqlDialect(DatabaseMetaData databaseMetaData)
    {
        try {
            identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        } catch (SQLException e) {
            throw Util.newInternal(e, "while quoting identifier");
        }
        identifierQuoteString = identifierQuoteString.trim();
        if (identifierQuoteString.equals("")) {
            identifierQuoteString = null;
        }
        try {
            databaseProductName = databaseMetaData.getDatabaseProductName();
        } catch (SQLException e) {
            throw Util.newInternal(e, "while detecting database product");
        }
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isAccess()
    {
        return databaseProductName.equals("ACCESS");
    }

    // -- detect various databases --
    public boolean isOracle()
    {
        return databaseProductName.equals("Oracle");
    }

    public boolean isPostgres()
    {
        return databaseProductName.toUpperCase().indexOf("POSTGRE") >= 0;
    }

    public boolean isSqlServer()
    {
        return databaseProductName.toUpperCase().indexOf("SQL SERVER") >= 0;
    }

    /**
     * Encloses an identifier in quotation marks appropriate for the current SQL
     * dialect.
     *
     * <p>For example, <code>quoteIdentifier("emp")</code> yields a string
     * containing <code>"emp"</code> in Oracle, and a string containing <code>
     * [emp]</code> in Access.
     *
     * @param val Identifier to quote
     *
     * @return Quoted identifier
     */
    public String quoteIdentifier(String val)
    {
        if (identifierQuoteString == null) {
            return val; // quoting is not supported
        }
        String val2 =
            val.replaceAll(
                identifierQuoteString,
                identifierQuoteString + identifierQuoteString);
        return identifierQuoteString + val2 + identifierQuoteString;
    }

    /**
     * Encloses an identifier in quotation marks appropriate for the current SQL
     * dialect, writing the result to a {@link StringBuilder}.
     *
     * <p>For example, <code>quoteIdentifier("emp")</code> yields a string
     * containing <code>"emp"</code> in Oracle, and a string containing <code>
     * [emp]</code> in Access.
     *
     * @param buf Buffer
     * @param val Identifier to quote
     *
     * @return The buffer
     */
    public StringBuilder quoteIdentifier(
        StringBuilder buf,
        String val)
    {
        if (identifierQuoteString == null) {
            buf.append(val); // quoting is not supported
            return buf;
        }
        String val2 =
            val.replaceAll(
                identifierQuoteString,
                identifierQuoteString + identifierQuoteString);
        buf.append(identifierQuoteString);
        buf.append(val2);
        buf.append(identifierQuoteString);
        return buf;
    }

    /**
     * Quotes a multi-part identifier.
     *
     * @param buf Buffer
     * @param identifiers List of parts of the identifier to quote
     *
     * @return The buffer
     */
    public StringBuilder quoteIdentifier(
        StringBuilder buf,
        List<String> identifiers)
    {
        int i = 0;
        for (String identifier : identifiers) {
            if (i++ > 0) {
                buf.append('.');
            }
            quoteIdentifier(buf, identifier);
        }
        return buf;
    }

    /**
     * Returns whether a given identifier needs to be quoted.
     */
    public boolean identifierNeedsToBeQuoted(String val)
    {
        return !Pattern.compile("^[A-Z_$0-9]+").matcher(val).matches();
    }

    /**
     * Converts a string into a string literal. For example, <code>can't
     * run</code> becomes <code>'can''t run'</code>.
     */
    public String quoteStringLiteral(String val)
    {
        val = Util.replace(val, "'", "''");
        return "'" + val + "'";
    }

    /**
     * Converts a string literal back into a string. For example, <code>'can''t
     * run'</code> becomes <code>can't run</code>.
     */
    public String unquoteStringLiteral(String val)
    {
        if ((val != null)
            && (val.charAt(0) == '\'')
            && (val.charAt(val.length() - 1) == '\''))
        {
            if (val.length() > 2) {
                val = Util.replace(val, "''", "'");
                return val.substring(1, val.length() - 1);
            } else {
                // zero length string
                return "";
            }
        }
        return val;
    }

    protected boolean allowsAs()
    {
        return !isOracle();
    }

    // -- behaviors --
    protected boolean requiresAliasForFromItems()
    {
        return isPostgres();
    }
}
// End SqlDialect.java
