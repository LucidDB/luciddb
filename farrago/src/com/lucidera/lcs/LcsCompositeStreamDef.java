/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lcs;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;


/**
 * LcsCompositeStreamDef is a composite Fem stream definition.
 *
 * <pre>
 * generator -> sorter -> splicer
 * </pre>
 *
 * @author John Pham
 * @version $Id$
 */
class LcsCompositeStreamDef
{

    //~ Instance fields --------------------------------------------------------

    private FemExecutionStreamDef consumer;
    private FemExecutionStreamDef producer;

    //~ Constructors -----------------------------------------------------------

    public LcsCompositeStreamDef(
        FemExecutionStreamDef consumer,
        FemExecutionStreamDef producer)
    {
        this.consumer = consumer;
        this.producer = producer;
    }

    //~ Methods ----------------------------------------------------------------

    public FemExecutionStreamDef getConsumer()
    {
        return consumer;
    }

    public FemExecutionStreamDef getProducer()
    {
        return producer;
    }
}

// End LcsCompositeStreamDef.java
