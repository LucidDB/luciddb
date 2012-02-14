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

import java.util.*;

import org.eigenbase.runtime.*;


/**
 * MedMockIterator generates mock data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMockIterator
    implements RestartableIterator
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
    public MedMockIterator(
        Object obj,
        long nRows)
    {
        this.obj = obj;
        this.nRowsInit = nRows;
        this.nRows = nRows;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Iterator
    public Object next()
    {
        --nRows;
        return obj;
    }

    // implement Iterator
    public boolean hasNext()
    {
        return nRows > 0;
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // implement RestartableIterator
    public void restart()
    {
        nRows = nRowsInit;
    }
}

// End MedMockIterator.java
