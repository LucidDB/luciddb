/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.sql;

import net.sf.saffron.util.Util;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;


/**
 * <code>SqlDialect</code> encapsulates the differences between dialects of
 * SQL, for the benefit of a {@link SqlWriter}.
 */
public class SqlDialect
{
    //~ Instance fields -------------------------------------------------------

    String databaseProductName;
    String identifierQuoteString;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>SqlDialect</code>
     *
     * @param databaseMetaData used to determine which dialect of SQL to
     *        generate
     */
    public SqlDialect(DatabaseMetaData databaseMetaData)
    {
        try {
            identifierQuoteString =
                databaseMetaData.getIdentifierQuoteString();
        } catch (SQLException e) {
            throw Util.newInternal(e,"while quoting identifier");
        }
        identifierQuoteString = identifierQuoteString.trim();
        if (identifierQuoteString.equals("")) {
            identifierQuoteString = null;
        }
        try {
            databaseProductName = databaseMetaData.getDatabaseProductName();
        } catch (SQLException e) {
            throw Util.newInternal(e,"while detecting database product");
        }
    }

    //~ Methods ---------------------------------------------------------------

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

    /**
     * Encloses an identifier in quotation marks appropriate for the current
     * SQL dialect. For example, <code>quoteIdentifier("emp")</code> yields a
     * string containing <code>"emp"</code> in Oracle, and a string
     * containing <code>[emp]</code> in Access.
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
     * Converts a string into a string literal. For example, <code>can't
     * run</code> becomes <code>'can''t run'</code>.
     */
    public String quoteStringLiteral(String val)
    {
        val = Util.replace(val,"'","''");
        return "'" + val + "'";
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
