/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.query;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;


/**
 * FennelPullRel defines the interface which must be implemented by any
 * {@link RelNode} subclass with {@link #FENNEL_PULL_CONVENTION}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FennelPullRel extends FennelRel
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Calling convention which transfers data by pulling rows in Fennel tuple
     * format (implementations must conform to the fennel::TupleStream
     * interface).
     */
    public static final CallingConvention FENNEL_PULL_CONVENTION =
        new CallingConvention("FENNEL_PULL",
            CallingConvention.generateOrdinal(), FennelRel.class);
}


// End FennelPullRel.java
