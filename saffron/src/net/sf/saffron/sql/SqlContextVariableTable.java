/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Technologies, Inc.
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
        mapNameToType.put(USER.getSimple(),varcharType);
        mapNameToType.put(CURRENT_USER.getSimple(),varcharType);
        mapNameToType.put(SYSTEM_USER.getSimple(),varcharType);
        mapNameToType.put(SESSION_USER.getSimple(),varcharType);
        mapNameToType.put(CURRENT_PATH.getSimple(),varcharType);
        mapNameToType.put(CURRENT_ROLE.getSimple(),varcharType);

        // TODO jvs 20-Feb-2004:  once Farrago supports temporal types
        /*
          
        SaffronType dateType = typeFactory.createSqlType(
            SqlTypeName.Date);

        // REVIEW jvs 20-Feb-2004:  SqlTypeName says Time and Timestamp
        // don't take precision, but they should (according to the standard).
        // Also, need to take care of time zones.
        
        SaffronType timeType = typeFactory.createSqlType(
            SqlTypeName.Time);
        
        SaffronType timestampType = typeFactory.createSqlType(
            SqlTypeName.Timestamp);
        
        mapNameToType.put(CURRENT_DATE.getSimple(),dateType);
        mapNameToType.put(CURRENT_TIME.getSimple(),timeType);
        mapNameToType.put(CURRENT_TIMESTAMP.getSimple(),timestampType);
        mapNameToType.put(LOCALTIME.getSimple(),timeType);
        mapNameToType.put(LOCALTIMESTAMP.getSimple(),timestampType);
        */
    }

    public SaffronType deriveType(SqlIdentifier id)
    {
        if (id.names.length != 1) {
            return null;
        }
        String name = id.getSimple();
        return (SaffronType) mapNameToType.get(name);
    }

    public static final SqlIdentifier USER
        = new SqlIdentifier("USER");

    public static final SqlIdentifier CURRENT_USER
        = new SqlIdentifier("CURRENT_USER");
    
    public static final SqlIdentifier SESSION_USER
        = new SqlIdentifier("SESSION_USER");
    
    public static final SqlIdentifier SYSTEM_USER
        = new SqlIdentifier("SYSTEM_USER");
    
    public static final SqlIdentifier CURRENT_PATH
        = new SqlIdentifier("CURRENT_PATH");
    
    public static final SqlIdentifier CURRENT_ROLE
        = new SqlIdentifier("CURRENT_ROLE");
    
    public static final SqlIdentifier CURRENT_DATE
        = new SqlIdentifier("CURRENT_DATE");
    
    // TODO jvs 20-Feb-2004:  The expressions below should
    // also allow function syntax (with the time precision as the parameter)
    
    public static final SqlIdentifier CURRENT_TIME
        = new SqlIdentifier("CURRENT_TIME");

    public static final SqlIdentifier CURRENT_TIMESTAMP
        = new SqlIdentifier("CURRENT_TIMESTAMP");
    
    public static final SqlIdentifier LOCALTIME
        = new SqlIdentifier("LOCALTIME");
    
    public static final SqlIdentifier LOCALTIMESTAMP
        = new SqlIdentifier("LOCALTIMESTAMP");
    
}

// End SqlContextVariableTable.java
