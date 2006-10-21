/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.runtime;

import java.nio.*;

import net.sf.farrago.fennel.tuple.*;

/**
 * FennelOnlyTupleReader implements the FennelTupleReader interface for reading
 * tuples from a query plan that can be executed exclusively in Fennel.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelOnlyTupleReader implements FennelTupleReader
{
    //~ Instance fields --------------------------------------------------------
    private final FennelTupleAccessor tupleAccessor;
    private final FennelTupleData tupleData;
    
    //~ Constructor ------------------------------------------------------------
    /**
     * @param tupleDesc tuple descriptor of the tuples to be read
     * @param tupleData tuple data that the tuples read will be unmarshalled
     * into
     */
    public FennelOnlyTupleReader(
        FennelTupleDescriptor tupleDesc,
        FennelTupleData tupleData)
    {
        tupleAccessor = new FennelTupleAccessor(true);
        tupleAccessor.compute(tupleDesc);
        this.tupleData = tupleData;
    }
        
    //~ Methods ----------------------------------------------------------------

    // implement FennelTupleReader
    public Object unmarshalTuple(
        ByteBuffer byteBuffer,
        byte [] byteArray,
        ByteBuffer sliceBuffer)
    {
        if (tupleAccessor.getCurrentTupleBuf() == null) {
            tupleAccessor.setCurrentTupleBuf(byteBuffer);
        }
        tupleAccessor.unmarshal(tupleData);
        return tupleData;
    }
}

// End FennelOnlyTupleReader.java
