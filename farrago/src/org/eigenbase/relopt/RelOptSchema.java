/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package org.eigenbase.relopt;

import org.eigenbase.reltype.*;

/**
 * A <code>RelOptSchema</code> is a set of {@link RelOptTable} objects.
 *
 * @see RelOptConnection
 */
public interface RelOptSchema
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Retrieves a {@link RelOptTable} based upon a member access.
     * 
     * <p>
     * For example, the Saffron expression <code>salesSchema.emps</code> would
     * be resolved using a call to
     * <code>salesSchema.getTableForMember(new String[]{"emps"})</code>.
     * </p>
     * 
     * <p> Note that name.length is only greater than 1 for queries originating
     * from JDBC.
     * </p>
     */
    RelOptTable getTableForMember(String [] names);

    /**
     * Returns the {@link RelDataTypeFactory type factory} used to generate
     * types for this schema.
     */
    RelDataTypeFactory getTypeFactory();

    /**
     * Registers all of the rules supported by this schema. Only called by
     * {@link RelOptPlanner#registerSchema}.
     */
    void registerRules(RelOptPlanner planner) throws Exception;
}


// End RelOptSchema.java
