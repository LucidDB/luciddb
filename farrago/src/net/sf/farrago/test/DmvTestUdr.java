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

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.dmv.*;
import org.eigenbase.enki.mdr.*;
import org.eigenbase.jmi.*;
import org.eigenbase.lurql.*;
import org.eigenbase.util.*;


/**
 * DmvTestUdr is a SQL-invocable entry point for package {@link
 * org.eigenbase.dmv}.
 *
 * <p>NOTE: this lives here rather than under org.eigenbase because it currently
 * depends on MDR for a JMI implementation.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class DmvTestUdr
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Executes a visualization transform via a procedure called from SQL,
     * producing a .dot file which can be used as input to Graphviz.
     *
     * @param foreignServerName name of predefined foreign server to use as
     * source; must be defined using the MDR foreign data wrapper
     * @param lurqlFilename name of file containing LURQL to execute
     * @param transformationFilename name of file containing rules for
     * transforming LURQL results into visualization input
     * @param dotFilename name of .dot file to create
     */
    public static void renderGraphviz(
        String foreignServerName,
        String lurqlFilename,
        String transformationFilename,
        String dotFilename)
        throws Exception
    {
        FarragoMdrTestContext context = new FarragoMdrTestContext();
        try {
            context.init(foreignServerName);
            renderGraphviz(
                context,
                lurqlFilename,
                transformationFilename,
                dotFilename);
        } finally {
            context.closeAllocation();
        }
    }

    public static void renderGraphviz(
        FarragoMdrTestContext context,
        String lurqlFilename,
        String transformationFilename,
        String dotFilename)
        throws Exception
    {
        lurqlFilename =
            FarragoProperties.instance().expandProperties(
                lurqlFilename);
        transformationFilename =
            FarragoProperties.instance().expandProperties(
                transformationFilename);
        dotFilename =
            FarragoProperties.instance().expandProperties(
                dotFilename);
        FileWriter dotWriter = new FileWriter(dotFilename);
        ((EnkiMDRepository) context.getMdrRepos()).beginSession();
        try {
            String lurql = readFileAsString(lurqlFilename);
            JmiQueryProcessor queryProcessor =
                new LurqlQueryProcessor(
                    context.getMdrRepos());
            JmiPreparedQuery query =
                queryProcessor.prepare(
                    context.getModelView(),
                    lurql);

            // TODO jvs 11-June-2006:  Configure loopback connection
            Collection<RefObject> searchResult = query.execute(null, null);
            JmiDependencyMappedTransform transform =
                new JmiDependencyMappedTransform(
                    context.getModelView(),
                    false);
            DmvTransformXmlReader xmlReader =
                new DmvTransformXmlReader(
                    context.getModelGraph());
            xmlReader.readTransformationRules(
                transformationFilename,
                transform);
            JmiDependencyGraph graph =
                new JmiDependencyGraph(
                    searchResult,
                    transform);
            DmvGraphvizRenderer renderer = new DmvGraphvizRenderer();
            DmvResponse response = new DmvResponse(searchResult, graph);
            renderer.renderDmv(
                response,
                dotWriter);
        } finally {
            dotWriter.close();
            ((EnkiMDRepository) context.getMdrRepos()).endSession();
        }
    }

    private static String readFileAsString(String filename)
        throws IOException
    {
        FileReader fileReader = new FileReader(filename);
        try {
            return Util.readAllAsString(fileReader);
        } finally {
            fileReader.close();
        }
    }
}

// End DmvTestUdr.java
