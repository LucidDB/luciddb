/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
package net.sf.farrago.test;

import java.io.*;

import java.sql.*;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.test.*;

import org.eigenbase.jmi.*;
import org.eigenbase.lurql.*;


/**
 * LurqlQueryUdx executes a LURQL query and returns the resulting collection as
 * a relational result set.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LurqlQueryUdx
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Executes a LURQL query, producing result columns as follows:
     *
     * <ol>
     * <li>CLASS_NAME: class name of matching object
     * <li>OBJ_NAME: name of matching object, or null if it lacks a "name"
     * attribute
     * <li>MOF_ID: MOFID of matching object
     * <li>OBJ_ATTRS: concatenation of attribute/value pairs defining matching
     * object
     * </ol>
     *
     * @param foreignServerName name of predefined foreign server to use as
     * source; must be defined using the MDR foreign data wrapper
     * @param lurql LURQL query to execute (may not contain parameters)
     * @param resultInserter used to write results
     */
    public static void queryMedMdr(
        String foreignServerName,
        String lurql,
        PreparedStatement resultInserter)
        throws Exception
    {
        FarragoMdrTestContext context = new FarragoMdrTestContext();
        try {
            context.init(foreignServerName);
            Map<String, String> argMap = new HashMap<String, String>();
            JmiQueryProcessor queryProcessor =
                new LurqlQueryProcessor(
                    context.getMdrRepos());
            JmiPreparedQuery query =
                queryProcessor.prepare(
                    context.getModelView(),
                    lurql);

            // TODO jvs 11-June-2006:  Configure loopback connection
            Collection results = query.execute(null, null);
            for (Object obj : results) {
                RefObject refObj = (RefObject) obj;
                SortedMap map = JmiObjUtil.getAttributeValues(refObj);
                resultInserter.setString(
                    1,
                    JmiObjUtil.getMetaObjectName(refObj));
                resultInserter.setString(
                    2,
                    (String) map.get("name"));
                resultInserter.setString(
                    3,
                    refObj.refMofId());
                resultInserter.setString(
                    4,
                    map.toString());
                resultInserter.executeUpdate();
            }
        } finally {
            context.closeAllocation();
        }
    }
}

// End LurqlQueryUdx.java
