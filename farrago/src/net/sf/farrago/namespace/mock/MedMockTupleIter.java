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
package net.sf.farrago.namespace.mock;

import org.eigenbase.runtime.*;


/**
 * MedMockTupleIter generates mock data.
 *
 * @author Stephan Zuercher (adapted from MedMockIterator)
 * @version $Id$
 */
public class MedMockTupleIter
    extends AbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    private Object obj;
    private long nRows;
    private long nRowsInit;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructor.
     *
     * @param obj the single object which is returned over and over
     * @param nRows number of rows to generate
     */
    public MedMockTupleIter(
        Object obj,
        long nRows)
    {
        this.obj = obj;
        this.nRowsInit = nRows;
        this.nRows = nRows;
    }

    //~ Methods ----------------------------------------------------------------

    // implement TupleIter
    public Object fetchNext()
    {
        if (nRows > 0) {
            --nRows;
            return obj;
        }

        return NoDataReason.END_OF_DATA;
    }

    // implement TupleIter
    public void restart()
    {
        nRows = nRowsInit;
    }

    // implement TupleIter
    public void closeAllocation()
    {
    }
}

// End MedMockTupleIter.java
