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

package net.sf.farrago.query;

import net.sf.farrago.runtime.*;
import net.sf.farrago.util.*;

import net.sf.saffron.oj.stmt.*;
import net.sf.saffron.core.*;

import java.sql.*;
import java.util.*;

/**
 * FarragoExecutableExplainStmt implements FarragoExecutableStmt for
 * an EXPLAIN PLAN statement.
 *
 *<p>
 *
 * NOTE:  be sure to read superclass warnings before modifying this class.
 *
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoExecutableExplainStmt extends FarragoExecutableStmtImpl
{
    private final String explanation;

    FarragoExecutableExplainStmt(
        SaffronType dynamicParamRowType,
        String explanation)
    {
        super(dynamicParamRowType,false);
        
        this.explanation = explanation;
    }

    // implement FarragoExecutableStmt
    public SaffronType getRowType()
    {
        // TODO:  make a proper type descriptor (and use it for execute also)
        throw new UnsupportedOperationException();
    }

    // implement FarragoExecutableStmt
    public ResultSet execute(
        FarragoRuntimeContext runtimeContext)
    {
        // don't need a context
        runtimeContext.closeAllocation();

        return PreparedExplanation.executeStatic(explanation);
    }

    // implement FarragoExecutableStmt
    public long getMemoryUsage()
    {
        return FarragoUtil.getStringMemoryUsage(explanation);
    }
}

// End FarragoExecutableExplainStmt.java
