/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2005 John V. Sichi.
// Copyright (C) 2003-2005 Disruptive Tech
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

import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;


/**
 * FennelRel defines the interface which must be implemented by any
 * {@link RelNode} corresponding to a C++ physical implementation conforming
 * to the fennel::ExecutionStream interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FennelRel extends RelNode
{
    //~ Methods ---------------------------------------------------------------

    // TODO jvs 8-May-2004:  get rid of method getPreparingStmt();
    // instead, add a utility method for getting it from the cluster

    /**
     * Converts this relational expression to {@link FemExecutionStreamDef}
     * form. In the process, the relational expression will almost certainly
     * call {@link FennelRelImplementor#visitFennelChild} on each of its
     * children.
     *
     * @param implementor for generating Java code
     *
     * @return generated FemExecutionStreamDef
     */
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor);

    /**
     * Visits this relational expression as part of the implementation
     * process. Fennel relational expressions are implemented in a two-phase
     * process: first call this method, then call {@link #toStreamDef}.
     */
    Object implementFennelChild(FennelRelImplementor implementor);

    /**
     * .
     *
     * @return the sort order produced by this FennelRel, or an empty array if
     * the output is not guaranteed to be in any particular order
     */
    public RelFieldCollation [] getCollations();
}


// End FennelRel.java
