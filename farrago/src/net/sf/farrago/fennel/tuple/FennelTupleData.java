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
package net.sf.farrago.fennel.tuple;

import java.util.*;


/**
 * FennelTupleData is an in-memory collection of independent data values, as
 * explained in the fennel tuple <a
 * href="http://fennel.sourceforge.net/doxygen/html/structTupleDesign.html">
 * design document</a>. This class is JDK 1.4 compatible.
 */
public class FennelTupleData
{
    //~ Instance fields --------------------------------------------------------

    /**
     * the TupleDatums we are responsible for.
     */
    private final List datums = new ArrayList();

    //~ Constructors -----------------------------------------------------------

    /**
     * default constructor.
     */
    public FennelTupleData()
    {
    }

    /**
     * creates a FennelTupleData object from a FennelTupleDescriptor.
     */
    public FennelTupleData(FennelTupleDescriptor tupleDesc)
    {
        compute(tupleDesc);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * obtains a FennelTupleDatum object given the index of an entry.
     */
    public FennelTupleDatum getDatum(int n)
    {
        return (FennelTupleDatum) datums.get(n);
    }

    /**
     * returns the number of datums we have.
     */
    public int getDatumCount()
    {
        return datums.size();
    }

    /**
     * adds a new FennelTupleDatum object.
     */
    public void add(FennelTupleDatum d)
    {
        datums.add(d);
    }

    /**
     * creates our FennelTupleDatum objects from a FennelTupleDescriptor.
     */
    public void compute(FennelTupleDescriptor tupleDesc)
    {
        datums.clear();
        int i;
        for (i = 0; i < tupleDesc.getAttrCount(); ++i) {
            FennelTupleAttributeDescriptor attrDesc = tupleDesc.getAttr(i);
            FennelTupleDatum datum = new FennelTupleDatum(attrDesc.storageSize);
            int ordinal = attrDesc.typeDescriptor.getOrdinal();
            switch (ordinal) {
            case FennelStandardTypeDescriptor.UNICODE_CHAR_ORDINAL:
            case FennelStandardTypeDescriptor.UNICODE_VARCHAR_ORDINAL:
                datum.setUnicode(true);
                break;
            }
            add(datum);
        }
    }

    /**
     * indicates whether this tuple contains any null FennelTupleDatum elements.
     */
    public boolean containsNull()
    {
        int i;
        for (i = 0; i < datums.size(); ++i) {
            if (!getDatum(i).isPresent()) {
                return true;
            }
        }
        return false;
    }
}

// End FennelTupleData.java
