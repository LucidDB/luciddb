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

import net.sf.saffron.util.Util;
import net.sf.saffron.sql.SqlOperatorTable;
import net.sf.saffron.sql.SqlOperator;

import java.lang.reflect.Field;
import java.util.HashMap;

/**
 * A <code>RexOperatorTable</code> is ...
 *
 * @author jhyde
 * @since Nov 29, 2003
 * @version $Id$
 **/
public class RexOperatorTable extends SqlOperatorTable {
    public static final RexOperatorTable instance = new RexOperatorTable();

    public final RexOperator andOperator =
        new RexOperator("AND",RexKind.And);
    public final RexOperator concatOperator =
        new RexOperator("||",RexKind.Concat);
    public final RexOperator divideOperator =
        new RexOperator("/",RexKind.Divide);
    public final RexOperator equalsOperator =
        new RexOperator("=",RexKind.Equals);
    public final RexOperator greaterThanOperator =
        new RexOperator(">",RexKind.GreaterThan);
    public final RexOperator greaterThanOrEqualOperator =
        new RexOperator(">=",RexKind.GreaterThanOrEqual);
    public final RexOperator lessThanOperator =
        new RexOperator("<",RexKind.LessThan);
    public final RexOperator lessThanOrEqualOperator =
        new RexOperator("<=",RexKind.LessThanOrEqual);
    public final RexOperator minusOperator =
        new RexOperator("-",RexKind.Minus);
    public final RexOperator multiplyOperator =
        new RexOperator("*",RexKind.Times);
    public final RexOperator notEqualsOperator =
        new RexOperator("<>",RexKind.NotEquals);
    public final RexOperator orOperator =
        new RexOperator("OR",RexKind.Or);
    public final RexOperator plusOperator =
        new RexOperator("+",RexKind.Plus);

    // function
    public final RexOperator substringFunction = new RexOperator("SUBSTRING",RexKind.Substr);
    public final RexOperator rowConstructor = new RexOperator("ROW",RexKind.Row);

    // postfix
    public final RexOperator isNullOperator =
        new RexOperator("IS NULL",RexKind.IsNull);
    public final RexOperator isTrueOperator =
        new RexOperator("IS TRUE",RexKind.IsTrue);

    // prefix
    public final RexOperator notOperator =
        new RexOperator("NOT",RexKind.Not);
    public final RexOperator prefixMinusOperator =
        new RexOperator("-",RexKind.MinusPrefix);

    private HashMap mapKindToOperator = new HashMap();

    //~ Constructors ----------------------------------------------------------

    private RexOperatorTable()
    {
        super();
        // Use reflection to register the expressions stored in public fields.
        Field [] fields = getClass().getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (RexOperator.class.isAssignableFrom(field.getType())) {
                try {
                    RexOperator op = (RexOperator) field.get(this);
                    register(op);
                } catch (IllegalArgumentException e) {
                    throw Util.newInternal(
                        e,
                        "Error while initializing operator table");
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(
                        e,
                        "Error while initializing operator table");
                }
            }
        }
    }

    private void register(RexOperator op) {
        if (op.kind != null) {
            mapKindToOperator.put(op.kind,op);
        }
    }

    public SqlOperator get(RexKind kind, RexNode[] args) {
        return (SqlOperator) mapKindToOperator.get(kind);
    }
}

// End RexOperatorTable.java
