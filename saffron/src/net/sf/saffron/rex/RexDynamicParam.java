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

import net.sf.saffron.core.*;

/**
 * Dynamic parameter reference in a row-expression.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RexDynamicParam extends RexVariable
{
    public final int index;

    /**
     * Creates a dynamic parameter.
     *
     * @param type inferred type of parameter
     *
     * @param index 0-based index of dynamic parameter in statement
     */
    public RexDynamicParam(SaffronType type,int index)
    {
        super("?" + index, type);
        this.index = index;
    }

    public Object clone() 
    {
        return new RexDynamicParam(type,index);
    }

    public RexKind getKind() {
        return RexKind.DynamicParam;
    }

    public void accept(RexVisitor visitor) {
        visitor.visitParam(this);
    }
}

// End RexDynamicParam.java
