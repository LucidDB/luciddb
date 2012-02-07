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
package net.sf.farrago.test;

import java.io.*;

import java.security.*;


/**
 * Mock JAAS principal class
 *
 * @author Oscar Gothberg
 * @version $Id$
 */

public class FarragoMockPrincipal
    implements Serializable,
        Principal
{
    //~ Instance fields --------------------------------------------------------

    private String name;

    //~ Constructors -----------------------------------------------------------

    public FarragoMockPrincipal()
    {
        name = "";
    }

    public FarragoMockPrincipal(String newName)
    {
        name = newName;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean equals(Object o)
    {
        if (o == null) {
            return false;
        }

        if (this == o) {
            return true;
        }

        if (o instanceof FarragoMockPrincipal) {
            if (((FarragoMockPrincipal) o).getName().equals(name)) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public int hashCode()
    {
        return name.hashCode();
    }

    public String toString()
    {
        return name;
    }

    public String getName()
    {
        return name;
    }
}

// End FarragoMockPrincipal.java
