/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Technologies, Inc.
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

import net.sf.saffron.sql.type.*;
import net.sf.saffron.sql.parser.ParserPosition;
import net.sf.saffron.core.*;

import java.util.*;

/**
 * SqlContextVariableTable defines names and types for context variables
 * such as SESSION_USER and CURRENT_TIMESTAMP.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlContextVariableTable
{
    private SaffronTypeFactory typeFactory;
    private Map mapNameToType;

    /**
     * Instantiate a SqlContextVariableTable.
     *
     * @param typeFactory SaffronTypeFactory to use for defining types
     * corresponding to variables.
     */
    public SqlContextVariableTable(SaffronTypeFactory typeFactory)
    {
        // REVIEW jvs 20-Feb-2004:  parameterize precision?
        SaffronType varcharType = typeFactory.createSqlType(
            SqlTypeName.Varchar,128);

        mapNameToType = new HashMap();
        mapNameToType.put(createUSER(null).getSimple(),varcharType);
        mapNameToType.put(createCURRENT_USER(null).getSimple(),varcharType);
        mapNameToType.put(createSYSTEM_USER(null).getSimple(),varcharType);
        mapNameToType.put(createSESSION_USER(null).getSimple(),varcharType);
        mapNameToType.put(createCURRENT_PATH(null).getSimple(),varcharType);
        mapNameToType.put(createCURRENT_ROLE(null).getSimple(),varcharType);


        SaffronType dateType = typeFactory.createSqlType(SqlTypeName.Date);

        // REVIEW jvs 20-Feb-2004:  SqlTypeName says Time and Timestamp
        // don't take precision, but they should (according to the standard).
        // Also, need to take care of time zones.

        SaffronType timeType = typeFactory.createSqlType(
            SqlTypeName.Time);

        SaffronType timestampType = typeFactory.createSqlType(
            SqlTypeName.Timestamp);

        mapNameToType.put(createCURRENT_DATE(null).getSimple(),dateType);
        mapNameToType.put(createCURRENT_TIME(null).getSimple(),timeType);
        mapNameToType.put(createCURRENT_TIMESTAMP(null).getSimple(),timestampType);
        mapNameToType.put(createLOCALTIME(null).getSimple(),timeType);
        mapNameToType.put(createLOCALTIMESTAMP(null).getSimple(),timestampType);

    }

    public SaffronType deriveType(SqlIdentifier id)
    {
        if (id.names.length != 1) {
            return null;
        }
        String name = id.getSimple();
        return (SaffronType) mapNameToType.get(name);
    }

    public static SqlIdentifier createUSER(ParserPosition parserPosition)
    {
        return new SqlIdentifier("USER", parserPosition);
    }

    public static SqlIdentifier createCURRENT_USER(ParserPosition parserPosition)
    {
        return new SqlIdentifier("CURRENT_USER", parserPosition);
    }

    public static SqlIdentifier createSESSION_USER(ParserPosition parserPosition)
    {
        return new SqlIdentifier("SESSION_USER", parserPosition);
    }

    public static final SqlIdentifier createSYSTEM_USER(ParserPosition parserPosition)
    {    return new SqlIdentifier("SYSTEM_USER",parserPosition);}

    public static final SqlIdentifier createCURRENT_PATH(ParserPosition parserPosition)
    {    return new SqlIdentifier("CURRENT_PATH",parserPosition);}

    public static final SqlIdentifier createCURRENT_ROLE(ParserPosition parserPosition)
    {    return new SqlIdentifier("CURRENT_ROLE",parserPosition);}

    public static final SqlIdentifier createCURRENT_DATE(ParserPosition parserPosition)
    {    return new SqlIdentifier("CURRENT_DATE",parserPosition);}

    // TODO jvs 20-Feb-2004:  The expressions below should
    // also allow function syntax (with the time precision as the parameter)

    public static final SqlIdentifier createCURRENT_TIME(ParserPosition parserPosition)
    {    return new SqlIdentifier("CURRENT_TIME",parserPosition);}

    public static final SqlIdentifier createCURRENT_TIMESTAMP(ParserPosition parserPosition)
    {    return new SqlIdentifier("CURRENT_TIMESTAMP",parserPosition);}

    public static final SqlIdentifier createLOCALTIME(ParserPosition parserPosition)
    {    return new SqlIdentifier("LOCALTIME",parserPosition);}

    public static final SqlIdentifier createLOCALTIMESTAMP(ParserPosition parserPosition)
    {    return new SqlIdentifier("LOCALTIMESTAMP",parserPosition);}

}

// End SqlContextVariableTable.java
