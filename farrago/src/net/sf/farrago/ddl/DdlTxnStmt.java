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
package net.sf.farrago.ddl;

/**
 * DdlTxnStmt is a base for all statements which control the transaction context
 * (e.g. COMMIT).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DdlTxnStmt
    extends DdlStmt
{
    //~ Constructors -----------------------------------------------------------

    public DdlTxnStmt()
    {
        super(null);
    }

    //~ Methods ----------------------------------------------------------------

    // override DdlStmt
    public boolean requiresCommit()
    {
        return false;
    }
}

// End DdlTxnStmt.java
