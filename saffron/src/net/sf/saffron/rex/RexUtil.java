/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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
package net.sf.saffron.rex;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronField;

/**
 * Utility methods concerning row-expressions.
 *
 * @author jhyde
 * @since Nov 23, 2003
 * @version $Id$
 **/
public class RexUtil {
    public static final RexNode [] emptyExpressionArray = new RexNode[0];

    public static double getSelectivity(RexNode exp)
    {
        return 0.5;
    }

    /**
     * Returns a copy of a row-expression.
     */
    public static RexNode clone(RexNode exp) {
        return (RexNode) exp.clone();
    }

    /**
     * Returns a copy of an array of row-expressions.
     */
    public static RexNode[] clone(RexNode[] exps) {
        RexNode[] exps2 = new RexNode[exps.length];
        for (int i = 0; i < exps.length; i++) {
            exps2[i] = clone(exps[i]);
        }
        return exps2;
    }

    /**
     * Generate a cast from one row type to another
     *
     * @param rexBuilder RexBuilder to use for constructing casts
     *
     * @param lhsRowType target row type
     *
     * @param rhsRowType source row type; fields must be 1-to-1 with
     * lhsRowType, in same order
     *
     * @return cast expressions
     */
    public static RexNode [] generateCastExpressions(
        RexBuilder rexBuilder,
        SaffronType lhsRowType,
        SaffronType rhsRowType)
    {
        int n = rhsRowType.getFieldCount();
        assert(n == lhsRowType.getFieldCount());
        RexNode [] rhsExps = new RexNode[n];
        for (int i = 0; i < n; ++i) {
            rhsExps[i] = rexBuilder.makeInputRef(
                rhsRowType.getFields()[i].getType(),
                i);
        }
        return generateCastExpressions(
            rexBuilder,
            lhsRowType,
            rhsExps);
    }

    /**
     * Generate a cast for a row type.
     *
     * @param rexBuilder RexBuilder to use for constructing casts
     *
     * @param lhsRowType target row type
     *
     * @param rhsExps expressions to be cast
     *
     * @return cast expressions
     */
    public static RexNode [] generateCastExpressions(
        RexBuilder rexBuilder, SaffronType lhsRowType, RexNode [] rhsExps)
    {
        final int fieldCount = lhsRowType.getFieldCount();
        RexNode [] castExps = new RexNode[fieldCount];
        assert fieldCount == rhsExps.length;
        SaffronField [] lhsFields = lhsRowType.getFields();
        for (int i = 0; i < fieldCount; ++i) {
            SaffronField lhsField = lhsFields[i];
            SaffronType lhsType = lhsField.getType();
            SaffronType rhsType = rhsExps[i].getType();
            if (lhsType.equals(rhsType)) {
                castExps[i] = rhsExps[i];
            } else {
                castExps[i] = rexBuilder.makeCast(lhsType, rhsExps[i]);
            }
        }
        return castExps;
    }
}

// End RexUtil.java
