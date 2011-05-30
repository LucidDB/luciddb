/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
