/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.type.*;


/**
 * SqlContextVariableTable defines names and types for context variables
 * such as SESSION_USER and CURRENT_TIMESTAMP.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlContextVariableTable
{
    //~ Instance fields -------------------------------------------------------

    private RelDataTypeFactory typeFactory;
    private Map mapNameToType;

    //~ Constructors ----------------------------------------------------------

    /**
     * Instantiate a SqlContextVariableTable.
     *
     * @param typeFactory RelDataTypeFactory to use for defining types
     * corresponding to variables.
     */
    public SqlContextVariableTable(RelDataTypeFactory typeFactory)
    {
        // REVIEW jvs 20-Feb-2004:  parameterize precision?
        RelDataType varcharType =
            typeFactory.createSqlType(SqlTypeName.Varchar, 128);

        mapNameToType = new HashMap();
        mapNameToType.put(
            createUSER(ParserPosition.ZERO).getSimple(),
            varcharType);
        mapNameToType.put(
            createCURRENT_USER(ParserPosition.ZERO).getSimple(),
            varcharType);
        mapNameToType.put(
            createSYSTEM_USER(ParserPosition.ZERO).getSimple(),
            varcharType);
        mapNameToType.put(
            createSESSION_USER(ParserPosition.ZERO).getSimple(),
            varcharType);
        mapNameToType.put(
            createCURRENT_PATH(ParserPosition.ZERO).getSimple(),
            varcharType);
        mapNameToType.put(
            createCURRENT_ROLE(ParserPosition.ZERO).getSimple(),
            varcharType);

        RelDataType dateType = typeFactory.createSqlType(SqlTypeName.Date);

        // REVIEW jvs 20-Feb-2004:  SqlTypeName says Time and Timestamp
        // don't take precision, but they should (according to the standard).
        // Also, need to take care of time zones.
        RelDataType timeType = typeFactory.createSqlType(SqlTypeName.Time);

        RelDataType timestampType =
            typeFactory.createSqlType(SqlTypeName.Timestamp);

        mapNameToType.put(
            createCURRENT_DATE(ParserPosition.ZERO).getSimple(),
            dateType);
        mapNameToType.put(
            createCURRENT_TIME(ParserPosition.ZERO).getSimple(),
            timeType);
        mapNameToType.put(
            createCURRENT_TIMESTAMP(ParserPosition.ZERO).getSimple(),
            timestampType);
        mapNameToType.put(
            createLOCALTIME(ParserPosition.ZERO).getSimple(),
            timeType);
        mapNameToType.put(
            createLOCALTIMESTAMP(ParserPosition.ZERO).getSimple(),
            timestampType);
    }

    //~ Methods ---------------------------------------------------------------

    public RelDataType deriveType(SqlIdentifier id)
    {
        if (id.names.length != 1) {
            return null;
        }
        String name = id.getSimple();
        return (RelDataType) mapNameToType.get(name);
    }

    public static SqlIdentifier createUSER(ParserPosition pos)
    {
        return new SqlIdentifier("USER", pos);
    }

    public static SqlIdentifier createCURRENT_USER(ParserPosition pos)
    {
        return new SqlIdentifier("CURRENT_USER", pos);
    }

    public static SqlIdentifier createSESSION_USER(ParserPosition pos)
    {
        return new SqlIdentifier("SESSION_USER", pos);
    }

    public static final SqlIdentifier createSYSTEM_USER(ParserPosition pos)
    {
        return new SqlIdentifier("SYSTEM_USER", pos);
    }

    public static final SqlIdentifier createCURRENT_PATH(ParserPosition pos)
    {
        return new SqlIdentifier("CURRENT_PATH", pos);
    }

    public static final SqlIdentifier createCURRENT_ROLE(ParserPosition pos)
    {
        return new SqlIdentifier("CURRENT_ROLE", pos);
    }

    public static final SqlIdentifier createCURRENT_DATE(ParserPosition pos)
    {
        return new SqlIdentifier("CURRENT_DATE", pos);
    }

    // TODO jvs 20-Feb-2004:  The expressions below should
    // also allow function syntax (with the time precision as the parameter)
    public static final SqlIdentifier createCURRENT_TIME(ParserPosition pos)
    {
        return new SqlIdentifier("CURRENT_TIME", pos);
    }

    public static final SqlIdentifier createCURRENT_TIMESTAMP(
        ParserPosition pos)
    {
        return new SqlIdentifier("CURRENT_TIMESTAMP", pos);
    }

    public static final SqlIdentifier createLOCALTIME(ParserPosition pos)
    {
        return new SqlIdentifier("LOCALTIME", pos);
    }

    public static final SqlIdentifier createLOCALTIMESTAMP(ParserPosition pos)
    {
        return new SqlIdentifier("LOCALTIMESTAMP", pos);
    }
}


// End SqlContextVariableTable.java
