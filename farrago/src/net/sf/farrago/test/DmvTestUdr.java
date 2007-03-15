/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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

import org.eigenbase.dmv.*;
import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import net.sf.farrago.session.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;

import java.io.*;
import java.util.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;

import com.lucidera.lurql.*;

/**
 * DmvTestUdr is a SQL-invocable entry point for package {@link
 * org.eigenbase.dmv}.
 *
 * <p>NOTE: this lives here rather than under org.eigenbase because it
 * currently depends on MDR for a JMI implementation.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class DmvTestUdr
{
    /**
     * Executes a visualization transform via a procedure called from SQL,
     * producing a .dot file which can be used as input to Graphviz.
     *
     * @param foreignServerName name of predefined foreign server to use as
     * source; must be defined using the MDR foreign data wrapper
     * @param lurqlFilename name of file containing LURQL to execute
     * @param transformationFilename name of file containing rules
     * for transforming LURQL results into visualization input
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
            DmvTransformXmlReader xmlReader = new DmvTransformXmlReader(
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
