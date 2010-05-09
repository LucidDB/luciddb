/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

import java.util.*;

import org.eigenbase.util.*;


/**
 * Class to hold rules to determine if a type is assignable from another type.
 *
 * <p>REVIEW 7/05/04 Wael: We should split this up in Cast rules, symmetric and
 * asymmetric assignable rules
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlTypeAssignmentRules
{
    //~ Static fields/initializers ---------------------------------------------

    private static SqlTypeAssignmentRules instance = null;
    private static Map<SqlTypeName, Set<SqlTypeName>> rules = null;
    private static Map<SqlTypeName, Set<SqlTypeName>> coerceRules = null;

    //~ Constructors -----------------------------------------------------------

    private SqlTypeAssignmentRules()
    {
        rules = new HashMap<SqlTypeName, Set<SqlTypeName>>();

        HashSet<SqlTypeName> rule;

        //IntervalYearMonth is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.INTERVAL_YEAR_MONTH);
        rules.put(SqlTypeName.INTERVAL_YEAR_MONTH, rule);

        //IntervalDayTime is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.INTERVAL_DAY_TIME);
        rules.put(SqlTypeName.INTERVAL_DAY_TIME, rule);

        // Multiset is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.MULTISET);
        rules.put(SqlTypeName.MULTISET, rule);

        // Tinyint is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rules.put(SqlTypeName.TINYINT, rule);

        // Smallint is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rules.put(SqlTypeName.SMALLINT, rule);

        // Int is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rules.put(SqlTypeName.INTEGER, rule);

        // BigInt is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rules.put(SqlTypeName.BIGINT, rule);

        // Float is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.FLOAT);
        rules.put(SqlTypeName.FLOAT, rule);

        // Real is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.FLOAT);
        rule.add(SqlTypeName.REAL);
        rules.put(SqlTypeName.REAL, rule);

        // Double is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.FLOAT);
        rule.add(SqlTypeName.REAL);
        rule.add(SqlTypeName.DOUBLE);
        rules.put(SqlTypeName.DOUBLE, rule);

        // Decimal is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.REAL);
        rule.add(SqlTypeName.DOUBLE);
        rule.add(SqlTypeName.DECIMAL);
        rules.put(SqlTypeName.DECIMAL, rule);

        // VarBinary is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.VARBINARY);
        rule.add(SqlTypeName.BINARY);
        rules.put(SqlTypeName.VARBINARY, rule);

        // Char is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.CHAR);
        rules.put(SqlTypeName.CHAR, rule);

        // VarChar is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.CHAR);
        rule.add(SqlTypeName.VARCHAR);
        rules.put(SqlTypeName.VARCHAR, rule);

        // Boolean is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.BOOLEAN);
        rules.put(SqlTypeName.BOOLEAN, rule);

        // Binary is assignable from...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.BINARY);
        rule.add(SqlTypeName.VARBINARY);
        rules.put(SqlTypeName.BINARY, rule);

        // Date is assignable from ...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.DATE);
        rule.add(SqlTypeName.TIMESTAMP);
        rules.put(SqlTypeName.DATE, rule);

        // Time is assignable from ...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.TIMESTAMP);
        rules.put(SqlTypeName.TIME, rule);

        // Timestamp is assignable from ...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TIMESTAMP);
        rules.put(SqlTypeName.TIMESTAMP, rule);

        // we use coerceRules when we're casting
        coerceRules = copy(rules);

        // Make numbers symmetrical and
        // make varchar/char castable to/from numbers
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.FLOAT);
        rule.add(SqlTypeName.REAL);
        rule.add(SqlTypeName.DOUBLE);

        rule.add(SqlTypeName.CHAR);
        rule.add(SqlTypeName.VARCHAR);

        coerceRules.put(
            SqlTypeName.TINYINT,
            copy(rule));
        coerceRules.put(
            SqlTypeName.SMALLINT,
            copy(rule));
        coerceRules.put(
            SqlTypeName.INTEGER,
            copy(rule));
        coerceRules.put(
            SqlTypeName.BIGINT,
            copy(rule));
        coerceRules.put(
            SqlTypeName.FLOAT,
            copy(rule));
        coerceRules.put(
            SqlTypeName.REAL,
            copy(rule));
        coerceRules.put(
            SqlTypeName.DECIMAL,
            copy(rule));
        coerceRules.put(
            SqlTypeName.DOUBLE,
            copy(rule));
        coerceRules.put(
            SqlTypeName.CHAR,
            copy(rule));
        coerceRules.put(
            SqlTypeName.VARCHAR,
            copy(rule));

        // Exact Numerics are castable from intervals
        for (SqlTypeName exactType : SqlTypeName.exactTypes) {
            rule = (HashSet<SqlTypeName>) coerceRules.get(exactType);
            rule.add(SqlTypeName.INTERVAL_DAY_TIME);
            rule.add(SqlTypeName.INTERVAL_YEAR_MONTH);
        }

        // intervals are castable from Exact Numeric
        rule =
            (HashSet<SqlTypeName>) coerceRules.get(
                SqlTypeName.INTERVAL_DAY_TIME);
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.VARCHAR);

        // intervals  castable from Exact Numeric
        rule =
            (HashSet<SqlTypeName>) coerceRules.get(
                SqlTypeName.INTERVAL_YEAR_MONTH);
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.VARCHAR);

        // varchar is castable from Boolean, Date, time, timestamp, numbers and
        // intervals
        rule = (HashSet<SqlTypeName>) coerceRules.get(SqlTypeName.VARCHAR);
        rule.add(SqlTypeName.BOOLEAN);
        rule.add(SqlTypeName.DATE);
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.TIMESTAMP);
        rule.add(SqlTypeName.INTERVAL_DAY_TIME);
        rule.add(SqlTypeName.INTERVAL_YEAR_MONTH);

        // char is castable from Boolean, Date, time and timestamp and numbers
        rule = (HashSet<SqlTypeName>) coerceRules.get(SqlTypeName.CHAR);
        rule.add(SqlTypeName.BOOLEAN);
        rule.add(SqlTypeName.DATE);
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.TIMESTAMP);
        rule.add(SqlTypeName.INTERVAL_DAY_TIME);
        rule.add(SqlTypeName.INTERVAL_YEAR_MONTH);

        // Boolean is castable from char and varchar
        rule = (HashSet<SqlTypeName>) coerceRules.get(SqlTypeName.BOOLEAN);
        rule.add(SqlTypeName.CHAR);
        rule.add(SqlTypeName.VARCHAR);

        // Date, time, and timestamp are castable from
        // char and varchar
        // Date is castable from ...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.DATE);
        rule.add(SqlTypeName.TIMESTAMP);
        rule.add(SqlTypeName.CHAR);
        rule.add(SqlTypeName.VARCHAR);
        coerceRules.put(SqlTypeName.DATE, rule);

        // Time is castable from ...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.TIMESTAMP);
        rule.add(SqlTypeName.CHAR);
        rule.add(SqlTypeName.VARCHAR);
        coerceRules.put(SqlTypeName.TIME, rule);

        // Timestamp is castable from ...
        rule = new HashSet<SqlTypeName>();
        rule.add(SqlTypeName.TIMESTAMP);
        rule.add(SqlTypeName.DATE);
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.CHAR);
        rule.add(SqlTypeName.VARCHAR);
        coerceRules.put(SqlTypeName.TIMESTAMP, rule);
    }

    //~ Methods ----------------------------------------------------------------

    public synchronized static SqlTypeAssignmentRules instance()
    {
        if (instance == null) {
            instance = new SqlTypeAssignmentRules();
        }
        return instance;
    }

    public boolean canCastFrom(
        SqlTypeName to,
        SqlTypeName from,
        boolean coerce)
    {
        assert (null != to);
        assert (null != from);

        Map<SqlTypeName, Set<SqlTypeName>> ruleset =
            coerce ? coerceRules : rules;

        if (to.equals(SqlTypeName.NULL)) {
            return false;
        } else if (from.equals(SqlTypeName.NULL)) {
            return true;
        }

        Set<SqlTypeName> rule = ruleset.get(to);
        if (null == rule) {
            // if you hit this assert, see the constructor of this class on how
            // to add new rule
            throw Util.newInternal(
                "No assign rules for " + to + " defined");
        }

        return rule.contains(from);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> HashMap<K, V> copy(Map<K, V> map)
    {
        HashMap<K, V> copy = new HashMap<K, V>();
        for (
            Iterator<Map.Entry<K, V>> i = map.entrySet().iterator();
            i.hasNext();)
        {
            Map.Entry<K, V> e = i.next();
            if (e.getValue() instanceof Set) {
                copy.put(e.getKey(), (V) copy((Set) e.getValue()));
            } else {
                copy.put(e.getKey(), e.getValue());
            }
        }
        return copy;
    }

    private static <T> HashSet<T> copy(Set<T> set)
    {
        return new HashSet<T>(set);
    }
}

// End SqlTypeAssignmentRules.java
