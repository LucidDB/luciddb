/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package org.eigenbase.sql.type;

import java.sql.*;

import org.eigenbase.util.*;
import org.eigenbase.reltype.*;


/**
 * SqlTypeFamily is a complete disjoint partitioning of SQL types into
 * families, where two types are memberss of the same family iff
 * instances of the two types can be the operands of an SQL equality
 * predicate such as <code>WHERE v1 = v2</code>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlTypeFamily
    extends EnumeratedValues.BasicValue
    implements RelDataTypeFamily
{
    //~ Static fields/initializers --------------------------------------------

    public static final int Character_ordinal = 0;
    public static final int Binary_ordinal = 1;
    public static final int Numeric_ordinal = 2;
    public static final int Date_ordinal = 3;
    public static final int Time_ordinal = 4;
    public static final int Timestamp_ordinal = 5;
    public static final int Boolean_ordinal = 6;
    public static final int IntervalYearMonth_ordinal = 7;
    public static final int IntervalDayTime_ordinal = 8;
    
    public static final SqlTypeFamily Character =
        new SqlTypeFamily("CHARACTER", Character_ordinal);
    public static final SqlTypeFamily Binary =
        new SqlTypeFamily("BINARY", Binary_ordinal);
    public static final SqlTypeFamily Numeric =
        new SqlTypeFamily("NUMERIC", Numeric_ordinal);
    public static final SqlTypeFamily Date =
        new SqlTypeFamily("DATE", Date_ordinal);
    public static final SqlTypeFamily Time =
        new SqlTypeFamily("TIME", Time_ordinal);
    public static final SqlTypeFamily Timestamp =
        new SqlTypeFamily("TIMESTAMP", Timestamp_ordinal);
    public static final SqlTypeFamily Boolean =
        new SqlTypeFamily("BOOLEAN", Boolean_ordinal);
    public static final SqlTypeFamily IntervalYearMonth =
        new SqlTypeFamily("INTERVAL_YEAR_MONTH", IntervalYearMonth_ordinal);
    public static final SqlTypeFamily IntervalDayTime =
        new SqlTypeFamily("INTERVAL_DAY_TIME", IntervalDayTime_ordinal);
    private static final SqlTypeFamily [] values =
        new SqlTypeFamily [] {
            Character, Binary, Numeric, Date, Time, Timestamp, Boolean,
            IntervalYearMonth, IntervalDayTime
        };
    private static SqlTypeFamily [] jdbcTypeToFamily;

    private static SqlTypeFamily [] sqlTypeToFamily;
    
    static {
        // This squanders some memory since MAX_JDBC_TYPE == 2006!
        jdbcTypeToFamily =
            new SqlTypeFamily[
                (1 + SqlTypeName.MAX_JDBC_TYPE) - SqlTypeName.MIN_JDBC_TYPE];

        setFamilyForJdbcType(Types.BIT, Numeric);
        setFamilyForJdbcType(Types.TINYINT, Numeric);
        setFamilyForJdbcType(Types.SMALLINT, Numeric);
        setFamilyForJdbcType(Types.BIGINT, Numeric);
        setFamilyForJdbcType(Types.INTEGER, Numeric);
        setFamilyForJdbcType(Types.NUMERIC, Numeric);
        setFamilyForJdbcType(Types.DECIMAL, Numeric);

        setFamilyForJdbcType(Types.FLOAT, Numeric);
        setFamilyForJdbcType(Types.REAL, Numeric);
        setFamilyForJdbcType(Types.DOUBLE, Numeric);

        setFamilyForJdbcType(Types.CHAR, Character);
        setFamilyForJdbcType(Types.VARCHAR, Character);
        setFamilyForJdbcType(Types.LONGVARCHAR, Character);
        setFamilyForJdbcType(Types.CLOB, Character);

        setFamilyForJdbcType(Types.BINARY, Binary);
        setFamilyForJdbcType(Types.VARBINARY, Binary);
        setFamilyForJdbcType(Types.LONGVARBINARY, Binary);
        setFamilyForJdbcType(Types.BLOB, Binary);

        setFamilyForJdbcType(Types.DATE, Date);
        setFamilyForJdbcType(Types.TIME, Time);
        setFamilyForJdbcType(Types.TIMESTAMP, Timestamp);
        setFamilyForJdbcType(Types.BOOLEAN, Boolean);
        
        sqlTypeToFamily =
            new SqlTypeFamily[SqlTypeName.enumeration.getMax() + 1];
        sqlTypeToFamily[SqlTypeName.Boolean_ordinal] = Boolean;
        sqlTypeToFamily[SqlTypeName.Char_ordinal] = Character;
        sqlTypeToFamily[SqlTypeName.Varchar_ordinal] = Character;
        sqlTypeToFamily[SqlTypeName.Binary_ordinal] = Binary;
        sqlTypeToFamily[SqlTypeName.Varbinary_ordinal] = Binary;
        sqlTypeToFamily[SqlTypeName.Decimal_ordinal] = Numeric;
        sqlTypeToFamily[SqlTypeName.Tinyint_ordinal] = Numeric;
        sqlTypeToFamily[SqlTypeName.Smallint_ordinal] = Numeric;
        sqlTypeToFamily[SqlTypeName.Integer_ordinal] = Numeric;
        sqlTypeToFamily[SqlTypeName.Bigint_ordinal] = Numeric;
        sqlTypeToFamily[SqlTypeName.Real_ordinal] = Numeric;
        sqlTypeToFamily[SqlTypeName.Double_ordinal] = Numeric;
        sqlTypeToFamily[SqlTypeName.Float_ordinal] = Numeric;
        sqlTypeToFamily[SqlTypeName.Date_ordinal] = Date;
        sqlTypeToFamily[SqlTypeName.Time_ordinal] = Time;
        sqlTypeToFamily[SqlTypeName.Timestamp_ordinal] = Timestamp;
        sqlTypeToFamily[SqlTypeName.IntervalYearMonth_ordinal] =
            IntervalYearMonth;
        sqlTypeToFamily[SqlTypeName.IntervalDayTime_ordinal] =
            IntervalDayTime;
    }

    /**
     * Enumeration of all families.
     */
    public static final EnumeratedValues enumeration =
        new EnumeratedValues(values);

    //~ Constructors ----------------------------------------------------------

    private SqlTypeFamily(
        String name,
        int ordinal)
    {
        super(name, ordinal, null);
    }

    //~ Methods ---------------------------------------------------------------

    private static void setFamilyForJdbcType(
        int jdbcType,
        SqlTypeFamily family)
    {
        jdbcTypeToFamily[jdbcType - SqlTypeName.MIN_JDBC_TYPE] = family;
    }

    /**
     * Gets the family containing a SqlTypeName.
     *
     * @param sqlTypeName the type of interest
     *
     * @return containing family, or null for none
     */
    public static SqlTypeFamily getFamilyForSqlType(SqlTypeName sqlTypeName)
    {
        return sqlTypeToFamily[sqlTypeName.getOrdinal()];
    }

    /**
     * Gets the family containing a JDBC type.
     *
     * @param jdbcType the JDBC type of interest
     *
     * @return containing family
     */
    public static SqlTypeFamily getFamilyForJdbcType(int jdbcType)
    {
        return jdbcTypeToFamily[jdbcType - SqlTypeName.MIN_JDBC_TYPE];
    }
}


// End SqlTypeFamily.java
