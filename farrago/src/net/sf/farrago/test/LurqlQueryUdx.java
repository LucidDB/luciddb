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
