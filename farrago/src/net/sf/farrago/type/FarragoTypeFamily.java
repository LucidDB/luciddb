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

package net.sf.farrago.type;

import org.eigenbase.util.*;
import java.sql.*;

// TODO:  i18n

/**
 * FarragoTypeFamily is a symbolic enumeration for the families of datatypes
 * supported by Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTypeFamily extends EnumeratedValues.BasicValue
{
    private FarragoTypeFamily(String name,int ordinal)
    {
        super(name,ordinal,null);
    }

    public static final FarragoTypeFamily CHARACTER =
    new FarragoTypeFamily("CHARACTER",0);

    public static final FarragoTypeFamily BINARY =
    new FarragoTypeFamily("BINARY",1);

    public static final FarragoTypeFamily NUMERIC =
    new FarragoTypeFamily("NUMERIC",2);

    public static final FarragoTypeFamily DATE =
    new FarragoTypeFamily("DATE",3);

    public static final FarragoTypeFamily TIME =
    new FarragoTypeFamily("TIME",4);

    public static final FarragoTypeFamily TIMESTAMP =
    new FarragoTypeFamily("TIMESTAMP",5);

    public static final FarragoTypeFamily BOOLEAN =
    new FarragoTypeFamily("BOOLEAN",6);
    
    private static final FarragoTypeFamily [] values =
    new FarragoTypeFamily [] {
        CHARACTER, BINARY, NUMERIC,
        DATE, TIME, TIMESTAMP, BOOLEAN
    };

    private static FarragoTypeFamily [] jdbcTypeToFamily;

    private static final int MIN_JDBC_TYPE = Types.BIT;
    private static final int MAX_JDBC_TYPE = Types.REF;
    
    static {
        // This squanders some memory since MAX_JDBC_TYPE == 2006!
        jdbcTypeToFamily =
            new FarragoTypeFamily[1 + MAX_JDBC_TYPE - MIN_JDBC_TYPE];
        
        setFamilyForJdbcType(Types.BIT,NUMERIC);
        setFamilyForJdbcType(Types.TINYINT,NUMERIC);
        setFamilyForJdbcType(Types.SMALLINT,NUMERIC);
        setFamilyForJdbcType(Types.BIGINT,NUMERIC);
        setFamilyForJdbcType(Types.INTEGER,NUMERIC);
        setFamilyForJdbcType(Types.NUMERIC,NUMERIC);
        setFamilyForJdbcType(Types.DECIMAL,NUMERIC);
        
        setFamilyForJdbcType(Types.FLOAT,NUMERIC);
        setFamilyForJdbcType(Types.REAL,NUMERIC);
        setFamilyForJdbcType(Types.DOUBLE,NUMERIC);

        setFamilyForJdbcType(Types.CHAR,CHARACTER);
        setFamilyForJdbcType(Types.VARCHAR,CHARACTER);
        setFamilyForJdbcType(Types.LONGVARCHAR,CHARACTER);
        setFamilyForJdbcType(Types.CLOB,CHARACTER);

        setFamilyForJdbcType(Types.BINARY,BINARY);
        setFamilyForJdbcType(Types.VARBINARY,BINARY);
        setFamilyForJdbcType(Types.LONGVARBINARY,BINARY);
        setFamilyForJdbcType(Types.BLOB,BINARY);
        
        setFamilyForJdbcType(Types.DATE,DATE);
        setFamilyForJdbcType(Types.TIME,TIME);
        setFamilyForJdbcType(Types.TIMESTAMP,TIMESTAMP);
        setFamilyForJdbcType(Types.BOOLEAN,BOOLEAN);
    }

    private static void setFamilyForJdbcType(
        int jdbcType,FarragoTypeFamily family)
    {
        jdbcTypeToFamily[jdbcType - MIN_JDBC_TYPE] = family;
    }

    /**
     * Get the family containing a JDBC type
     *
     * @param jdbcType the JDBC type of interest
     *
     * @return containing family
     */
    public static FarragoTypeFamily getFamilyForJdbcType(int jdbcType)
    {
        return jdbcTypeToFamily[jdbcType - MIN_JDBC_TYPE];
    }

    /**
     * Enumeration of all families.
     */
    public static final EnumeratedValues enumeration =
        new EnumeratedValues(values);
}

// End FarragoTypeFamily.java
