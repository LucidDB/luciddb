/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.core;

import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;


/**
 * Represents a relational dataset in a {@link SaffronSchema}.
 * It has methods to describe and implement itself.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 10 November, 2001
 */
public interface SaffronTable
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Obtain an identifier for this table.  The identifier must be unique
     * with respect to the Connection producing this table.
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
    SaffronType getRowType();

    /**
     * Returns the {@link SaffronSchema} this table belongs to.
     */
    SaffronSchema getSaffronSchema();

    /**
     * Converts this table into a {@link SaffronRel relational expression}.
     *
     * <p>The {@link net.sf.saffron.opt.VolcanoPlanner planner} calls this
     * method to convert a table into an initial relational expression,
     * generally something abstract, such as a
     * {@link net.sf.saffron.rel.TableAccessRel}, then optimizes this expression
     * by applying {@link net.sf.saffron.opt.VolcanoRule rules} to transform it
     * into more efficient access methods for this table.</p>
     *
     * @param cluster the cluster the relational expression will belong to
     * @param connection the parse tree of the expression which evaluates
     *        to a connection object
     * @pre cluster != null
     * @pre connection != null
     */
    SaffronRel toRel(VolcanoCluster cluster,SaffronConnection connection);
}


// End SaffronTable.java
