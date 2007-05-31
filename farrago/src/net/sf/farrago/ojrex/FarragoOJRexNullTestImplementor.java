/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.ojrex;

import net.sf.farrago.type.runtime.*;

import openjava.ptree.*;

import org.eigenbase.rex.*;


/**
 * FarragoOJRexNullTestImplementor implements Farrago specifics of
 * {@link org.eigenbase.oj.rex.OJRexImplementor} for null-test row expressions
 * <code>IS NULL</code> and <code>IS NOT NULL</code>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexNullTestImplementor
    extends FarragoOJRexImplementor
{

    //~ Instance fields --------------------------------------------------------

    private boolean isNull;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexNullTestImplementor(boolean isNull)
    {
        this.isNull = isNull;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        if (call.operands[0].getType().isNullable()) {
            Expression expr =
                new MethodCall(
                    operands[0],
                    NullableValue.NULL_IND_ACCESSOR_NAME,
                    new ExpressionList());
            if (isNull) {
                return expr;
            } else {
                return new UnaryExpression(UnaryExpression.NOT, expr);
            }
        } else {
            if (isNull) {
                return Literal.constantFalse();
            } else {
                return Literal.constantTrue();
            }
        }
    }
}

// End FarragoOJRexNullTestImplementor.java
