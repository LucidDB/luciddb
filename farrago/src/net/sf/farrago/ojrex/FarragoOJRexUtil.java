/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

import net.sf.farrago.type.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;


/**
 * Utility functions for generating OpenJava expressions.
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoOJRexUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a field access, as in expr.[value] for a primitive
     */
    public static Expression getValueAccessExpression(
        FarragoRexToOJTranslator translator,
        RelDataType type,
        Expression expr)
    {
        FarragoTypeFactory factory =
            (FarragoTypeFactory) translator.getTypeFactory();
        return factory.getValueAccessExpression(type, expr);
    }
}

// End FarragoOJRexUtil.java
