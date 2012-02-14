/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
public class FennelOnlyTupleReader
    implements FennelTupleReader
{
    //~ Instance fields --------------------------------------------------------

    private final FennelTupleAccessor tupleAccessor;
    private final FennelTupleData tupleData;

    //~ Constructors -----------------------------------------------------------

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
