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

package net.sf.saffron.oj.rex;

import net.sf.saffron.sql.*;
import net.sf.saffron.rex.*;

import openjava.ptree.*;

/**
 * OJRexBinaryExpressionImplementor implements {@link OJRexImplementor} for row
 * expressions which can be translated to instances of OpenJava {@link
 * BinaryExpression}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class OJRexBinaryExpressionImplementor implements OJRexImplementor
{
    private final int ojBinaryExpressionOrdinal;
    
    public OJRexBinaryExpressionImplementor(
        int ojBinaryExpressionOrdinal)
    {
        this.ojBinaryExpressionOrdinal = ojBinaryExpressionOrdinal;
    }
    
    public Expression implement(
        RexToOJTranslator translator,RexCall call,Expression [] operands)
    {
        return new BinaryExpression(
            operands[0],
            ojBinaryExpressionOrdinal,
            operands[1]);
    }
    
    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End OJRexBinaryExpressionImplementor.java
