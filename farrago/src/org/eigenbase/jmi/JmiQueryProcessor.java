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
package org.eigenbase.jmi;

import java.util.*;

import javax.jmi.reflect.*;

// REVIEW jvs 21-May-2005:  Make this a heavyweight allocation?


/**
 * JmiQueryProcessor defines an interface for preparing and executing queries
 * against JMI data. It does not specify the actual query language to use.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface JmiQueryProcessor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Prepares a query for execution.
     *
     * @param modelView a view of the model to be queried
     * @param query the text of the query (expected language is
     * implementation-dependent)
     *
     * @return reference to prepared query
     */
    public JmiPreparedQuery prepare(JmiModelView modelView, String query)
        throws JmiQueryException;
}

// End JmiQueryProcessor.java
