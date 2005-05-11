/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.farrago.FarragoMetadataFactory;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelImplementor;
import org.eigenbase.reltype.RelDataType;


/**
 * Callback used to hold state while converting a tree of {@link FennelRel}
 * objects into a plan consisting of {@link FemExecutionStreamDef} objects.
 *
 * @see FarragoRelImplementor
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public interface FennelRelImplementor extends RelImplementor
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Converts a relational expression into a plan by calling its
     * {@link FennelRel#toStreamDef} method.
     */
    public FemExecutionStreamDef visitFennelChild(FennelRel rel);

    /**
     * Registers a new stream definition.  Normally, it is not necessary
     * to call this method explicitly; it happens automatically as part
     * of visitFennelChild.  However, this is not true for non-tree stream
     * graphs.  For streams with multiple parents, this method must be called
     * for streams not returned from visitFennelChild.
     *
     * @param streamDef new stream definition
     *
     * @param rel RelNode which stream implements
     *
     * @param rowType row type for stream, or null to use rel's row type
     */
    public void registerRelStreamDef(
        FemExecutionStreamDef streamDef,
        RelNode rel,
        RelDataType rowType);

    /**
     * Returns the metadata factory.
     */
    public FarragoMetadataFactory getMetadataFactory();
}


// End FennelRelImplementor.java
