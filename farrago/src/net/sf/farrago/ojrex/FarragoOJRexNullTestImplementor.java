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

package net.sf.farrago.ojrex;

import net.sf.farrago.type.runtime.*;

import org.eigenbase.sql.*;
import org.eigenbase.rex.*;

import openjava.ptree.*;

/**
 * FarragoOJRexNullTestImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for null-test row expressions IS NULL and IS NOT NULL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexNullTestImplementor extends FarragoOJRexImplementor
{
    private boolean isNull;
    
    public FarragoOJRexNullTestImplementor(boolean isNull)
    {
        this.isNull = isNull;
    }
    
    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,RexCall call,Expression [] operands)
    {
        if (call.operands[0].getType().isNullable()) {
            Expression expr = new MethodCall(
                operands[0],
                NullableValue.NULL_IND_ACCESSOR_NAME,
                new ExpressionList());
            if (isNull) {
                return expr;
            } else {
                return new UnaryExpression(
                    UnaryExpression.NOT,
                    expr);
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
