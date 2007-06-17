/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.disruptivetech.farrago.calc;

import org.eigenbase.sql.*;


/**
 * Contains, for each operator, an implementor which can convert a call to that
 * operator into a set of calculator instructions.
 *
 * @author jhyde
 * @version $Id$
 * @since June 2nd, 2004
 */
public interface CalcRexImplementorTable
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the implementor of an operator, or null if there is no
     * implementor registered.
     */
    CalcRexImplementor get(SqlOperator op);

    /**
     * Retrieves the implementor of an aggregate function.
     */
    CalcRexAggImplementor getAgg(SqlAggFunction op);
}

// End CalcRexImplementorTable.java
