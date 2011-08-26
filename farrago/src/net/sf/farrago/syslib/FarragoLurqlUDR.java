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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;
import org.netbeans.api.mdr.*;
import org.netbeans.api.xmi.*;

/**
 * UDR which executes an arbitrary LURQL query and returns results as XMI for
 * remote use.
 *
 * @author chard
 */
public abstract class FarragoLurqlUDR
{
    private static final Logger tracer =
        Logger.getLogger(FarragoLurqlUDR.class.getName());
    private static JmiJsonUtil jmiJsonUtil = null;

    /**
     * Retrieves a set of JMI objects from the repository based on a LURQL
     * query.
     * @param lurql String containing LURQL query
     * @return Collection of RefObject instances representing the query
     * results
     */
    private static Collection<RefObject> getObjects(String lurql)
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoRepos repos = FarragoUdrRuntime.getRepos();
        FarragoReposTxnContext txn =
            repos.newTxnContext(true);
        txn.beginReadTxn();
        Collection<RefObject> results = null;
        try {
            results = session.executeLurqlQuery(lurql, null);
            tracer.fine("Read " + results.size() + " objects");
        } finally {
            txn.commit();
        }
        return results;
    }

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
        try {
            final Collection<RefObject> results = getObjects(lurql);
            result = JmiObjUtil.exportToXmiString(
                Collections.unmodifiableCollection(results));
            String[] chunks = StringChunker.slice(result);
            StringChunker.writeChunks(chunks, resultInserter);
        } catch (SQLException e) {
            tracer.warning("Problem writing objects to XMI: " + e.getMessage());
            tracer.warning("Stack trace: " + Util.getStackTrace(e));
        }
    }

    public static void getJSON(String lurql, PreparedStatement resultInserter)
    {
        try {
            final Collection<RefObject> results = getObjects(lurql);
            String json = getJmiJsonUtil().generateJSON(
                Collections.unmodifiableCollection(results));
            tracer.info("JSON =\n" + json);
            String[] chunks = StringChunker.slice(json);
            StringChunker.writeChunks(chunks, resultInserter);
        } catch (SQLException e) {
            tracer.warning("Problem writing JSON objects: " + e.getMessage());
            tracer.warning("Stack trace: " + Util.getStackTrace(e));
        }
    }

    private static synchronized JmiJsonUtil getJmiJsonUtil()
    {
        if (jmiJsonUtil == null) {
            FarragoSessionPersonality personality =
                FarragoUdrRuntime.getSession().getPersonality();
            jmiJsonUtil = personality.newJmiJsonUtil();
        }
        return jmiJsonUtil;
    }

    /**
     * Retrieves an XMI string representing the repository's metamodel,
     * basically by dumping the contents of the metamodel extent.
     * @param extentName String containing the name of the extant that holds
     * the metamodel
     * @param resultInserter PreparedStatement to receive the XMI output in
     * string chunks
     */
    public static void getMetamodelXMI(
        String extentName,
        PreparedStatement resultInserter)
    {
        MDRepository mdrRepos = FarragoUdrRuntime.getRepos().getMdrRepos();
        OutputStream out = new ByteArrayOutputStream(32768);
        try {
            RefPackage refPackage = mdrRepos.getExtent(extentName);
            XMIWriter xmiWriter =
                XMIWriterFactory.getDefault().createXMIWriter();
            xmiWriter.getConfiguration().setEncoding("UTF-8");
            try {
                xmiWriter.write(out, refPackage, "1.2");
            } finally {
                out.close();
            }
            String[] chunks = StringChunker.slice(out.toString());
            StringChunker.writeChunks(chunks, resultInserter);
        } catch (Exception e) {
            tracer.warning("Problem getting Metamodel XMI: " + e.getMessage());
            tracer.warning("Stack trace: " + Util.getStackTrace(e));
        }
    }
}

// End FarragoLurqlUDR.java
