/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;


/**
 * Represents a relational dataset in a {@link RelOptSchema}. It has methods to
 * describe and implement itself.
 *
 * @author jhyde
 * @version $Id$
 * @since 10 November, 2001
 */
public interface RelOptTable
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Obtains an identifier for this table. The identifier must be unique with
     * respect to the Connection producing this table.
     *
     * @return qualified name
     */
    String [] getQualifiedName();

    /**
     * Returns an estimate of the number of rows in the table.
     */
    double getRowCount();

    /**
     * Describes the type of rows returned by this table.
     */
    RelDataType getRowType();

    /**
     * Returns the {@link RelOptSchema} this table belongs to.
     */
    RelOptSchema getRelOptSchema();

    /**
     * Converts this table into a {@link RelNode relational expression}.
     *
     * <p>The {@link org.eigenbase.relopt.RelOptPlanner planner} calls this
     * method to convert a table into an initial relational expression,
     * generally something abstract, such as a {@link
     * org.eigenbase.rel.TableAccessRel}, then optimizes this expression by
     * applying {@link org.eigenbase.relopt.RelOptRule rules} to transform it
     * into more efficient access methods for this table.</p>
     *
     * @param cluster the cluster the relational expression will belong to
     * @param connection the parse tree of the expression which evaluates to a
     * connection object
     *
     * @pre cluster != null
     * @pre connection != null
     */
    RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection);

    /**
     * Returns a description of the physical ordering (or orderings) of the rows
     * returned from this table.
     *
     * @see RelNode#getCollationList()
     * @post return != null
     */
    public List<RelCollation> getCollationList();
}

// End RelOptTable.java
