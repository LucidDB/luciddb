/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Technologies, Inc.
// Copyright (C) 2002-2004 John V. Sichi
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

package org.eigenbase.oj.rex;

import org.eigenbase.rex.*;

import openjava.ptree.*;

/**
 * OJRexIgnoredCallImplementor implements {@link OJRexImplementor} by
 * completely ignoring a call and returning its one and only operand.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class OJRexIgnoredCallImplementor implements OJRexImplementor
{
    public Expression implement(
        RexToOJTranslator translator,RexCall call,Expression [] operands)
    {
        assert(operands.length == 1);
        return operands[0];
    }
    
    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End OJRexIgnoredCallImplementor.java
