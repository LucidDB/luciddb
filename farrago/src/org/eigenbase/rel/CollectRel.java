/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package org.eigenbase.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * A relational expression which collapses multiple rows into one.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link net.sf.farrago.fennel.rel.FarragoMultisetSplitterRule}
 * creates a CollectRel from a call to {@link
 * org.eigenbase.sql.fun.SqlMultisetValueConstructor} or to {@link
 * org.eigenbase.sql.fun.SqlMultisetQueryConstructor}.</li>
 * </ul>
 * </p>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Dec 12, 2004
 */
public final class CollectRel
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final String fieldName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a CollectRel.
     *
     * @param cluster Cluster
     * @param child Child relational expression
     * @param fieldName Name of the sole output field
     */
    public CollectRel(
        RelOptCluster cluster,
        RelNode child,
        String fieldName)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child);
        this.fieldName = fieldName;
    }

    //~ Methods ----------------------------------------------------------------

    // override Object (public, does not throw CloneNotSupportedException)
    public CollectRel clone()
    {
        CollectRel clone =
            new CollectRel(
                getCluster(),
                getChild().clone(),
                fieldName);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    /**
     * Returns the name of the sole output field.
     *
     * @return name of the sole output field
     */
    public String getFieldName()
    {
        return fieldName;
    }

    protected RelDataType deriveRowType()
    {
        return deriveCollectRowType(this, fieldName);
    }

    /**
     * Derives the output type of a collect relational expression.
     *
     * @param rel relational expression
     * @param fieldName name of sole output field
     *
     * @return output type of a collect relational expression
     */
    public static RelDataType deriveCollectRowType(
        SingleRel rel,
        String fieldName)
    {
        RelDataType childType = rel.getChild().getRowType();
        assert (childType.isStruct());
        RelDataType ret =
            SqlTypeUtil.createMultisetType(
                rel.getCluster().getTypeFactory(),
                childType,
                false);
        ret =
            rel.getCluster().getTypeFactory().createStructType(
                new RelDataType[] { ret },
                new String[] { fieldName });
        return rel.getCluster().getTypeFactory().createTypeWithNullability(
            ret,
            false);
    }
}

// End CollectRel.java
