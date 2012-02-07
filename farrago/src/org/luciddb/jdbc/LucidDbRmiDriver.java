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
package org.luciddb.jdbc;

import net.sf.farrago.jdbc.client.*;


/**
 * LucidDbRmiDriver is a JDBC driver for the LucidDB server for use by remote
 * clients communicating via RMI. It is based on VJDBC.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbRmiDriver
    extends FarragoUnregisteredVjdbcClientDriver
{
    //~ Static fields/initializers ---------------------------------------------

    static {
        new LucidDbRmiDriver().register();
    }

    //~ Methods ----------------------------------------------------------------

    // override FarragoAbstractJdbcDriver
    public String getBaseUrl()
    {
        // REVIEW jvs 3-Feb-2008:  This is a hack to allow the JDBC driver
        // to be loaded off of the bootstrap class path, which is not really
        // a good thing to do.  It's half-baked because it doesn't take
        // care of other resources normally loaded from
        // FarragoRelease.properties such as default port number.  The
        // problem is that the bootstrap class loader is so "primordial"
        // that it doesn't even know how to load resources.
        // If this hack stays around, it should be applied to the other
        // LucidDB JDBC drivers as well.
        return "jdbc:luciddb:";
    }
}

// End LucidDbRmiDriver.java
