/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

import org.eigenbase.util.*;

import java.util.*;

/**
 * Class to hold rules to determine if a type is assignable from another
 * type.
 *
 *<p>
 *
 * REVIEW 7/05/04 Wael: We should split this up in
 * Cast rules, symmetric and asymmetric assignable rules
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlTypeAssignmentRules
{
    private static SqlTypeAssignmentRules instance = null;
    private static HashMap rules = null;
    private static HashMap coerceRules = null;

    private SqlTypeAssignmentRules()
    {
        rules = new HashMap();

        HashSet rule;

        //IntervalYearMonth is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.IntervalYearMonth);
        rules.put(SqlTypeName.IntervalYearMonth, rule);

        //IntervalDayTime is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.IntervalDayTime);
        rules.put(SqlTypeName.IntervalDayTime, rule);

        // Multiset is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Multiset);
        rules.put(SqlTypeName.Multiset, rule);

        // Tinyint is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rules.put(SqlTypeName.Tinyint, rule);

        // Smallint is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rule.add(SqlTypeName.Smallint);
        rules.put(SqlTypeName.Smallint, rule);

        // Int is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rule.add(SqlTypeName.Smallint);
        rule.add(SqlTypeName.Integer);
        rules.put(SqlTypeName.Integer, rule);

        // BigInt is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rule.add(SqlTypeName.Smallint);
        rule.add(SqlTypeName.Integer);
        rule.add(SqlTypeName.Bigint);
        rules.put(SqlTypeName.Bigint, rule);

        // Float is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rule.add(SqlTypeName.Smallint);
        rule.add(SqlTypeName.Integer);
        rule.add(SqlTypeName.Bigint);
        rule.add(SqlTypeName.Decimal);
        rule.add(SqlTypeName.Float);
        rules.put(SqlTypeName.Float, rule);

        // Real is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rule.add(SqlTypeName.Smallint);
        rule.add(SqlTypeName.Integer);
        rule.add(SqlTypeName.Bigint);
        rule.add(SqlTypeName.Decimal);
        rule.add(SqlTypeName.Float);
        rule.add(SqlTypeName.Real);
        rules.put(SqlTypeName.Real, rule);

        // Double is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rule.add(SqlTypeName.Smallint);
        rule.add(SqlTypeName.Integer);
        rule.add(SqlTypeName.Bigint);
        rule.add(SqlTypeName.Decimal);
        rule.add(SqlTypeName.Float);
        rule.add(SqlTypeName.Real);
        rule.add(SqlTypeName.Double);
        rules.put(SqlTypeName.Double, rule);

        // Decimal is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rule.add(SqlTypeName.Smallint);
        rule.add(SqlTypeName.Integer);
        rule.add(SqlTypeName.Bigint);
        rule.add(SqlTypeName.Real);
        rule.add(SqlTypeName.Double);
        rule.add(SqlTypeName.Decimal);
        rules.put(SqlTypeName.Decimal, rule);

        // VarBinary is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Varbinary);
        rule.add(SqlTypeName.Binary);
        rules.put(SqlTypeName.Varbinary, rule);

        // Char is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Char);
        rules.put(SqlTypeName.Char, rule);

        // VarChar is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Char);
        rule.add(SqlTypeName.Varchar);
        rules.put(SqlTypeName.Varchar, rule);

        // Boolean is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Boolean);
        rules.put(SqlTypeName.Boolean, rule);

        // Binary is assignable from...
        rule = new HashSet();
        rule.add(SqlTypeName.Binary);
        rule.add(SqlTypeName.Varbinary);
        rules.put(SqlTypeName.Binary, rule);

        // Date is assignable from ...
        rule = new HashSet();
        rule.add(SqlTypeName.Date);
        rule.add(SqlTypeName.Timestamp);
        rules.put(SqlTypeName.Date, rule);

        // Time is assignable from ...
        rule = new HashSet();
        rule.add(SqlTypeName.Time);
        rule.add(SqlTypeName.Timestamp);
        rules.put(SqlTypeName.Time, rule);

        // Timestamp is assignable from ...
        rule = new HashSet();
        rule.add(SqlTypeName.Timestamp);
        rules.put(SqlTypeName.Timestamp, rule);

        // we use coerceRules when we're casting
        coerceRules = (HashMap) rules.clone();

        // Make numbers symmetrical and
        // make varchar/char castable to/from numbers
        rule = new HashSet();
        rule.add(SqlTypeName.Tinyint);
        rule.add(SqlTypeName.Smallint);
        rule.add(SqlTypeName.Integer);
        rule.add(SqlTypeName.Bigint);
        rule.add(SqlTypeName.Decimal);
        rule.add(SqlTypeName.Float);
        rule.add(SqlTypeName.Real);
        rule.add(SqlTypeName.Double);

        rule.add(SqlTypeName.Char);
        rule.add(SqlTypeName.Varchar);

        coerceRules.put(SqlTypeName.Tinyint, rule.clone());
        coerceRules.put(SqlTypeName.Smallint, rule.clone());
        coerceRules.put(SqlTypeName.Integer, rule.clone());
        coerceRules.put(SqlTypeName.Bigint, rule.clone());
        coerceRules.put(SqlTypeName.Float, rule.clone());
        coerceRules.put(SqlTypeName.Real, rule.clone());
        coerceRules.put(SqlTypeName.Decimal, rule.clone());
        coerceRules.put(SqlTypeName.Double, rule.clone());
        coerceRules.put(SqlTypeName.Char, rule.clone());
        coerceRules.put(SqlTypeName.Varchar, rule.clone());

        // varchar is castable from Boolean, Date, time and timestamp and numbers
        rule = (HashSet) coerceRules.get(SqlTypeName.Varchar);
        rule.add(SqlTypeName.Boolean);
        rule.add(SqlTypeName.Date);
        rule.add(SqlTypeName.Time);
        rule.add(SqlTypeName.Timestamp);

        // char is castable from Boolean, Date, time and timestamp and numbers
        rule = (HashSet) coerceRules.get(SqlTypeName.Char);
        rule.add(SqlTypeName.Boolean);
        rule.add(SqlTypeName.Date);
        rule.add(SqlTypeName.Time);
        rule.add(SqlTypeName.Timestamp);

        // Boolean is castable from char and varchar
        rule = (HashSet) coerceRules.get(SqlTypeName.Boolean);
        rule.add(SqlTypeName.Char);
        rule.add(SqlTypeName.Varchar);

        // Date, time, and timestamp are castable from
        // char and varchar
        // Date is assignable from ...
        rule = new HashSet();
        rule.add(SqlTypeName.Date);
        rule.add(SqlTypeName.Timestamp);
        rule.add(SqlTypeName.Char);
        rule.add(SqlTypeName.Varchar);
        coerceRules.put(SqlTypeName.Date, rule);

        // Time is assignable from ...
        rule = new HashSet();
        rule.add(SqlTypeName.Time);
        rule.add(SqlTypeName.Timestamp);
        rule.add(SqlTypeName.Char);
        rule.add(SqlTypeName.Varchar);
        coerceRules.put(SqlTypeName.Time, rule);

        rule = new HashSet();
        rule.add(SqlTypeName.Timestamp);
        rule.add(SqlTypeName.Char);
        rule.add(SqlTypeName.Varchar);
        coerceRules.put(SqlTypeName.Timestamp, rule);

        // REVIEW jvs 13-Dec-2004:  getting the milliseconds?
        // That sounds like a physical operation, and has nothing to do
        // with enforcing the logical rules.

        // for getting the milliseconds.
        // Bigint is assignable from...
        rule = new HashSet();
        rule = (HashSet) coerceRules.get(SqlTypeName.Bigint);
        // xluo 24-Sept-2005 datetime added, there is a specific 
        // test case for that. I assume it should.
        rule.add(SqlTypeName.Date);
        rule.add(SqlTypeName.Time);
        rule.add(SqlTypeName.Timestamp);
        coerceRules.put(SqlTypeName.Bigint, rule);
    }

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

        HashMap ruleset = coerce ? coerceRules : rules;

        if (to.equals(SqlTypeName.Null)) {
            return false;
        } else if (from.equals(SqlTypeName.Null)) {
            return true;
        }

        HashSet rule = (HashSet) ruleset.get(to);
        if (null == rule) {
            // if you hit this assert, see the constructor of this class on how
            // to add new rule
            throw Util.newInternal("No assign rules for " + to
                + " defined");
        }

        return rule.contains(from);
    }
}

// End SqlTypeAssignmentRules.java
