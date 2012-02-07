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

import javax.jmi.model.*;
import javax.jmi.reflect.*;


/**
 * JmiClassVertex represents a class in a JMI model.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiClassVertex
{
    //~ Instance fields --------------------------------------------------------

    private final MofClass mofClass;
    Class<? extends RefObject> javaInterface;
    RefClass refClass;

    //~ Constructors -----------------------------------------------------------

    JmiClassVertex(MofClass mofClass)
    {
        this.mofClass = mofClass;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the MofClass represented by this vertex
     */
    public MofClass getMofClass()
    {
        return mofClass;
    }

    /**
     * @return the RefClass represented by this vertex
     */
    public RefClass getRefClass()
    {
        return refClass;
    }

    /**
     * @return the Java interface represented by this vertex
     */
    public Class<? extends RefObject> getJavaInterface()
    {
        return javaInterface;
    }

    // implement Object
    public String toString()
    {
        return mofClass.getName();
    }

    // implement Object
    public int hashCode()
    {
        return mofClass.hashCode();
    }

    // implement Object
    public boolean equals(Object obj)
    {
        if (!(obj instanceof JmiClassVertex)) {
            return false;
        }
        return mofClass.equals(((JmiClassVertex) obj).mofClass);
    }
}

// End JmiClassVertex.java
