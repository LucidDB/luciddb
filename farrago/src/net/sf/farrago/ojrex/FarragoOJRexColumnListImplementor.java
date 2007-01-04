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

import openjava.ptree.*;
import openjava.mop.*;

import org.eigenbase.rex.*;


/**
 * FarragoOJRexColumnListImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for a ColumnList constructor
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoOJRexColumnListImplementor
    extends FarragoOJRexImplementor
{

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {          
        // allocate an OJClass object corresponding to a Java list and declare
        // a variable for that class
        OJClass listClass = OJClass.forClass(java.util.ArrayList.class);
        Variable variable =
            translator.createScratchVariableWithExpression(
                listClass,
                new AllocationExpression(
                    TypeName.forOJClass(listClass),
                    null,
                    null));
        
        // generate calls to add the individual column names to the list
        // object
        RexNode [] columns = (RexNode []) call.getOperands();
        for (int i = 0; i < columns.length; i++) {
            translator.addStatement(
                new ExpressionStatement(
                    new MethodCall(
                        variable,
                        "add",
                        new ExpressionList(
                            Literal.makeLiteral(
                                RexLiteral.stringValue(columns[i]))))));
        }
        
        return variable;
    }
}

// End FarragoOJRexColumnListImplementor.java
