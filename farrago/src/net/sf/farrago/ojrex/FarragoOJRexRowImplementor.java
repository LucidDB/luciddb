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

import net.sf.saffron.core.*;
import net.sf.saffron.sql.*;
import net.sf.saffron.rex.*;

import openjava.ptree.*;

/**
 * FarragoOJRexRowImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for ROW constructors.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexRowImplementor extends FarragoOJRexImplementor
{
    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,RexCall call,Expression [] operands)
    {
        SaffronType rowType = call.getType();
        Variable variable = translator.createScratchVariable(rowType);
        SaffronField [] fields = rowType.getFields();
        for (int i = 0; i < operands.length; ++i) {
            translator.convertCastOrAssignment(
                fields[i].getType(),
                call.operands[i].getType(),
                // TODO jvs 27-May-2004:  proper field name translation
                new FieldAccess(variable,fields[i].getName()),
                operands[i]);
        }
        return variable;
    }
}

// End FarragoOJRexRowImplementor.java
