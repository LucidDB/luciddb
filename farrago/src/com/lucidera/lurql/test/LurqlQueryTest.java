/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lurql.test;

import com.lucidera.lurql.*;
import com.lucidera.lurql.parser.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import net.sf.farrago.test.*;
import net.sf.farrago.util.*;

import junit.framework.*;
import junit.*;

import java.util.*;
import java.io.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;

/**
 * LurqlQueryTest is a JUnit harness for executing tests which are
 * implemented by running a script of LURQL queries and diffing the
 * output against a reference file containing the expected results.
 * MOF serves as both the metamodel and the model to be queried.
 * The script format is fairly limited; see the .lurql files
 * in the test suite for details (TODO:  link).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlQueryTest extends FarragoSqlTest
{
    public LurqlQueryTest(String testName)
        throws Exception
    {
        super(testName);
    }
    
    // implement TestCase
    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetUnitlurql.get(true),
            new FarragoSqlTestFactory() {
                public FarragoTestCase createSqlTest(String testName)
                    throws Exception
                {
                    return new LurqlQueryTest(testName);
                }
            });
    }

    // override FarragoSqlTest
    protected void runTest()
        throws Exception
    {
        // mask out source control Id
        addDiffMask("\\$Id.*\\$");

        RefPackage mofPackage = repos.getMdrRepos().getExtent("MOF");
        JmiModelGraph modelGraph = new JmiModelGraph(mofPackage);
        JmiModelView modelView = new JmiModelView(modelGraph);
        
        assert(getName().endsWith(".lurql"));
        File fileSansExt =
            new File(getName().substring(0, getName().length() - 6));
        OutputStream outputStream =
            openTestLogOutputStream(fileSansExt);
        
        FileReader reader = new FileReader(getName());
        Writer writer = new OutputStreamWriter(outputStream);
        PrintWriter pw = new PrintWriter(writer);

        LineNumberReader lineReader = new LineNumberReader(reader);
        StringBuffer sb = null;
        String action = null;
        for (;;) {
            String line = lineReader.readLine();
            if ((line != null) && (line.startsWith("#"))) {
                pw.println(line);
                continue;
            }
            if (action != null) {
                if ((line == null) || (line.trim().equals(""))) {
                    try {
                        executeAction(modelView, action, sb.toString(), pw);
                    } finally {
                        pw.println("****");
                        pw.println();
                    }
                    action = null;
                } else {
                    sb.append(line);
                    sb.append("\n");
                }
            } else {
                if (line == null) {
                    break;
                }
                if (line.trim().equals("")) {
                    pw.println(line);
                } else {
                    action = line;
                    sb = new StringBuffer();
                }
            }
        }

        pw.close();
        reader.close();
        writer.close();
        
        diffTestLog();
    }

    private void executeAction(
        JmiModelView modelView, String action,
        String queryString, PrintWriter pw)
        throws Exception
    {
        boolean explain = false;
        boolean execute = false;

        if (action.equals("EXPLAIN AND EXECUTE")) {
            explain = true;
            execute = true;
        } else if (action.equals("EXECUTE")) {
            execute = true;
        } else if (action.equals("EXPLAIN")) {
            explain = true;
        } else if (!action.equals("PARSE")) {
            throw new IllegalArgumentException(action);
        }

        LurqlParser parser =
            new LurqlParser(new StringReader(queryString));
        LurqlQuery query;
        try {
            query = parser.LurqlQuery();
        } catch (Throwable ex) {
            pw.println("PARSE INPUT:");
            pw.print(queryString);
            pw.println("PARSE ERROR:  " + ex.getMessage());
            return;
        }
        pw.println("PARSE RESULT:");
        pw.println(query.toString());

        if (explain || execute) {
            LurqlPlan plan;
            try {
                plan = new LurqlPlan(
                    repos.getMdrRepos(),
                    modelView,
                    query);
            } catch (Throwable ex) {
                pw.println("PREPARATION ERROR:  " + ex.getMessage());
                pw.println();
                return;
            }

            if (explain) {
                pw.println("EXPLANATION:");
                List list = new ArrayList();
                list.addAll(plan.getGraph().vertexSet());
                list.addAll(plan.getGraph().edgeSet());
                Collections.sort(list, new StringRepresentationComparator());
                Iterator iter = list.iterator();
                while (iter.hasNext()) {
                    pw.println(iter.next());
                }
                pw.println();
            }

            if (execute) {
                LurqlReflectiveExecutor executor =
                    new LurqlReflectiveExecutor(plan, connection);
                Set set;
                try {
                    set = executor.execute();
                } catch (Throwable ex) {
                    pw.println("EXECUTION ERROR:  " + ex.getMessage());
                    pw.println();
                    return;
                }
                pw.println("EXECUTION RESULT:");
                List result = new ArrayList();
                Iterator iter = set.iterator();
                while (iter.hasNext()) {
                    // We're querying MOF, so we know that everything coming
                    // back will be some kind of ModelElement.
                    ModelElement element = (ModelElement) iter.next();
                    String typeName =
                        ((ModelElement) element.refMetaObject()).getName();
                    result.add(typeName + ": " + element.getName());
                }
                Collections.sort(result);
                iter = result.iterator();
                while (iter.hasNext()) {
                    pw.println(iter.next());
                }
                pw.println();
            }
        }
    }
}

// End LurqlQueryTest.java
