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
package com.lucidera.luciddb.mbean.resource;

/**
 * Contains a singleton instance of {@link MBeanQuery} class for default
 * locale.  Note: this is a workaround since MBeanQuery.instance() has
 * problems loading the bundle from the jar.
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class MBeanQueryObject
{
    private static final MBeanQuery res;

    static
    {
        try {
            res = new MBeanQuery();
        } catch (Throwable ex) {
            throw new Error(ex);
        }
    }

    public static MBeanQuery get() {
        return res;
    }
}

// End MBeanQueryObject.java
