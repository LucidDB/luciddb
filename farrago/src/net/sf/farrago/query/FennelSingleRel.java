/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelSingleRel is a {@link FennelRel} corresponding to {@link SingleRel},
 * and which only takes a FennelRel as input.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelSingleRel extends SingleRel implements FennelRel
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelSingleRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel
     */
    protected FennelSingleRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, child);
    }

    //~ Methods ---------------------------------------------------------------

    // implement FennelRel
    public FarragoPreparingStmt getPreparingStmt()
    {
        return getFennelInput().getPreparingStmt();
    }

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) cluster.typeFactory;
    }

    /**
     * .
     *
     * @return repos for object definitions
     */
    public FarragoRepos getRepos()
    {
        return getPreparingStmt().getRepos();
    }

    /**
     * NOTE:  this method is intentionally private because interactions
     * between FennelRels must be mediated by FarragoRelImplementor.
     *
     * @return this rel's input, which must already have been
     * converted to a FennelRel
     */
    private FennelRel getFennelInput()
    {
        return (FennelRel) child;
    }

    // default implementation for FennelRel
    public RelFieldCollation [] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(this, 0, child);
    }
}


// End FennelSingleRel.java
