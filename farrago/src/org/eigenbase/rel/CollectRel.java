/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
 * <li>{@link com.disruptivetech.farrago.rel.FarragoMultisetSplitterRule}
 * creates a CollectRel from a call to {@link SqlMultisetValueConstructor} or to
 * {@link SqlMultisetQueryConstructor}.</li>
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

    public String getFieldName()
    {
        return fieldName;
    }

    protected RelDataType deriveRowType()
    {
        return deriveCollectRowType(this, fieldName);
    }

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
        return
            rel.getCluster().getTypeFactory().createTypeWithNullability(
                ret,
                false);
    }
}

// End CollectRel.java
