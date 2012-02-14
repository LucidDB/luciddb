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

/**
 * JmiQueryException specifies an exception thrown during JMI query processing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiQueryException
    extends Exception
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new exception.
     *
     * @param message description of exception
     */
    public JmiQueryException(
        String message)
    {
        this(message, null);
    }

    /**
     * Constructs a new exception with an underlying cause.
     *
     * @param message description of exception
     * @param cause underlying cause
     */
    public JmiQueryException(
        String message,
        Throwable cause)
    {
        super(message, cause);
    }
}

// End JmiQueryException.java
