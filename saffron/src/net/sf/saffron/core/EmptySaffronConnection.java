/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.core;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * A saffron connection which doesn't have any objects available.
 *
 * <p>A statement which uses this connection will only be able to reference
 * statement parameters.</p>
 *
 * @see RelOptConnection
 *
 * @author jhyde
 * @since Nov 28, 2003
 * @version $Id$
 **/
public class EmptySaffronConnection implements RelOptConnection
{
    private final EmptyRelOptSchema schema = new EmptyRelOptSchema();

    public RelOptSchema getRelOptSchema()
    {
        return schema;
    }

    public Object contentsAsArray(
        String qualifier,
        String tableName)
    {
        return null;
    }

    private static class EmptyRelOptSchema implements RelOptSchema
    {
        private final RelDataTypeFactoryImpl typeFactory =
            new SqlTypeFactoryImpl();

        public RelOptTable getTableForMember(String [] names)
        {
            return null;
        }

        public RelDataTypeFactory getTypeFactory()
        {
            return typeFactory;
        }

        public void registerRules(RelOptPlanner planner)
            throws Exception
        {
        }
    }
}


// End EmptySaffronConnection.java
