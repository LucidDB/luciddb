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
package net.sf.farrago.catalog;

import javax.jmi.reflect.*;


/**
 * FarragoReposIntegrityErr records one integrity error detected by {@link
 * FarragoRepos#verifyIntegrity}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoReposIntegrityErr
{
    //~ Instance fields --------------------------------------------------------

    private final String description;

    private final JmiException exception;

    private final RefObject refObj;

    //~ Constructors -----------------------------------------------------------

    public FarragoReposIntegrityErr(
        String description,
        JmiException exception,
        RefObject refObj)
    {
        this.description = description;
        this.exception = exception;
        this.refObj = refObj;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return description of the error
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * @return underlying exception reported by JMI, or null if failed integrity
     * rule was specific to Farrago
     */
    public JmiException getJmiException()
    {
        return exception;
    }

    /**
     * @return object on which error was detected, or null if error is not
     * specific to an object
     */
    public RefObject getRefObject()
    {
        return refObj;
    }

    // implement Object
    public String toString()
    {
        return description;
    }
}

// End FarragoReposIntegrityErr.java
