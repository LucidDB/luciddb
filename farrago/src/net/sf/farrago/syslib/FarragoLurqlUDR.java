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
package net.sf.farrago.syslib;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

/**
 * UDR which executes an arbitrary LURQL query and returns results as XMI for
 * remote use.
 *
 * @author chard
 */
public abstract class FarragoLurqlUDR
{
    private static final Logger tracer =
        Logger.getLogger("net.sf.farrago.syslib.FarragoLurqlUDR");
    /**
     * Executes a LURQL query and returns the resulting objcts as an XMI string.
     * Because XMI is quite verbose, we assume the results are huge, and
     * therefore return the XMI string in manageable chunks.
     * @param lurql String containing LURQL query
     * @param resultInserter PreparedStatement to receive the XMI output in
     * string chunks
     */
    public static void getXMI(String lurql, PreparedStatement resultInserter)
    {
        String result = "Nothing";
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoRepos repos = FarragoUdrRuntime.getRepos();
        FarragoReposTxnContext txn =
            repos.newTxnContext(true);
        txn.beginReadTxn();
        try {
            try {
                final Collection<RefObject> results =
                    session.executeLurqlQuery(lurql, null);
                tracer.fine("Read " + results.size() + " objects");
                result = JmiObjUtil.exportToXmiString(
                    Collections.unmodifiableCollection(results));
                String[] chunks = StringChunker.slice(result);
                StringChunker.writeChunks(chunks, resultInserter);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } finally {
            txn.commit();
        }
    }
}

// End FarragoLurqlUDR.java
