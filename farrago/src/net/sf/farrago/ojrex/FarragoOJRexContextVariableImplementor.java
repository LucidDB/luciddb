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

import org.eigenbase.oj.rex.OJRexImplementor;
import org.eigenbase.oj.rex.RexToOJTranslator;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rex.RexCall;
import org.eigenbase.reltype.RelDataType;
import openjava.ptree.*;
import net.sf.farrago.type.runtime.AssignableValue;


/**
 * Implements Farrago specifics of {@link OJRexImplementor} for context
 * variables such as "USER" and "SESSION_USER".
 *
 * <p>For example, "USER" becomes "connection.getContextVariable_USER()".
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FarragoOJRexContextVariableImplementor implements OJRexImplementor
{
    private final String name;

    public FarragoOJRexContextVariableImplementor(String name) {
        this.name = name;
    }

    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression[] operands)
    {
        return convertVariable(
            (FarragoRexToOJTranslator) translator,
            call.getType(),
            "getContextVariable_" + name);
    }

    private Expression convertVariable(
        FarragoRexToOJTranslator translator,
        RelDataType type,
        String accessorName)
    {
        Variable variable = translator.createScratchVariable(type);
        Variable connectionVariable =
            translator.getRelImplementor().getConnectionVariable();
        translator.addStatement(
            new ExpressionStatement(
                new MethodCall(
                    variable,
                    AssignableValue.ASSIGNMENT_METHOD_NAME,
                    new ExpressionList(
                        new MethodCall(
                            connectionVariable,
                            accessorName,
                            new ExpressionList())))));
        return new CastExpression(
            OJUtil.typeToOJClass(type),
            variable);
    }
    
    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End FarragoOJRexContextVariableImplementor.java
