/*
// Farrago is a relational database management system.
// (C) Copyright 2004-2004 Disruptive Tech
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

import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.rel.SaffronRel;

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
public interface FennelRelImplementor extends RelImplementor {
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
     * @param rel SaffronRel which stream implements
     */
    public void registerRelStreamDef(
        FemExecutionStreamDef streamDef,
        SaffronRel rel);
}

// End FennelRelImplementor.java
