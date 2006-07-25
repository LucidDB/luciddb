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

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FarragoOJRexRowImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for ROW and UDT constructors.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexRowImplementor
    extends FarragoOJRexImplementor
{

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        RelDataType rowType = call.getType();
        Variable variable = translator.createScratchVariable(rowType);
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < operands.length; ++i) {
            final RelDataTypeField field = fields[i];
            translator.convertCastOrAssignment(
                translator.getRepos().getLocalizedObjectName(
                    fields[i].getName()),
                fields[i].getType(),
                call.operands[i].getType(),
                translator.convertFieldAccess(variable, field),
                operands[i]);
        }
        return variable;
    }
}

// End FarragoOJRexRowImplementor.java
