/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

package org.eigenbase.relopt;


/**
 * A connection to a database.
 *
 * <p>A connection contains a {@link RelOptSchema}, via which the query
 * planner can access {@link RelOptTable} objects.</p>
 *
 * <p>If Saffron is being used as a Java preprocessor, every class which
 * implements <code>RelOptConnection</code> must implement the
 * method<blockquote><pre>public static RelOptSchema getRelOptSchema()</pre>
 * </blockquote></p>
 *
 * @see RelOptConnectionDecorator
 * @see EmptyRelOptConnection
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 10 November, 2001
 */
public interface RelOptConnection
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the schema underlying this connection.  Non-abstract classes
     * implementing this interface must also provide <code>public static
     * Schema getRelOptSchemaStatic()</code>.
     */
    RelOptSchema getRelOptSchema();

    /**
     * In theory, this method returns the contents of <code>tableName</code>
     * as an array; in practice, it is a placeholder recognized by the
     * optimizer to do something much more efficient. This involves calling
     * <code>{@link
     * RelOptSchema#getTableForMember}(qualifier,tableName).{@link
     * RelOptTable#toRel}(cluster, exp)</code>.
     */
    Object contentsAsArray(
        String qualifier,
        String tableName);
}


// End RelOptConnection.java
