/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
public abstract class FennelSingleRel
    extends SingleRel
    implements FennelRel
{
    //~ Constructors -----------------------------------------------------------

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
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child);
    }

    /**
     * Creates a new FennelSingleRel object with specific traits.
     *
     * @param cluster RelOptCluster for this rel
     * @param traits traits for this rel
     * @param child input rel
     */
    protected FennelSingleRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child)
    {
        super(
            cluster,
            traits,
            child);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) getCluster().getTypeFactory();
    }

    /**
     * NOTE: this method is intentionally private because interactions between
     * FennelRels must be mediated by FarragoRelImplementor.
     *
     * @return this rel's input, which must already have been converted to a
     * FennelRel
     */
    private FennelRel getFennelInput()
    {
        return (FennelRel) getChild();
    }

    // default implementation for FennelRel
    public RelFieldCollation [] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(
            this,
            0,
            getChild());
    }
}

// End FennelSingleRel.java
