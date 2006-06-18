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

import net.sf.farrago.test.*;
import net.sf.farrago.util.*;

import junit.framework.*;

import java.util.*;
import java.io.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;

/**
 * LurqlQueryTest is a JUnit harness for executing tests which are implemented
 * by running a script of LURQL queries and diffing the output against a
 * reference file containing the expected results.  By default, MOF serves as
 * both the metamodel and the model to be queried; this can be changed within a
 * script.  The script format is fairly limited; see the .lurql files in the
 * test suite for details (TODO: link).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlQueryTest extends FarragoSqlTest
{
    private JmiModelView modelView;

    private Map args;
    
    public LurqlQueryTest(String testName)
        throws Exception
    {
        super(testName);

        args = new HashMap();
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

        modelView = loadModelView("MOF");
        
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
                        executeAction(action, sb.toString(), pw);
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

    private JmiModelView loadModelView(String extentName)
    {
        RefPackage mofPackage = repos.getMdrRepos().getExtent(extentName);
        JmiModelGraph modelGraph = new JmiModelGraph(mofPackage);
        return new JmiModelView(modelGraph);
    }

    private void executeAction(
        String action,
        String queryString, PrintWriter pw)
        throws Exception
    {
        boolean explain = false;
        boolean execute = false;

        if (action.startsWith("EXTENT ")) {
            pw.println(action);
            String extentName = action.substring(7);
            modelView = loadModelView(extentName);
            return;
        } else if (action.startsWith("PARAM_VALUE ")) {
            String paramName = action.substring(12);
            args.put(paramName, queryString.trim());
            pw.println(action);
            pw.println(queryString);
            return;
        } else if (action.startsWith("PARAM_VALUES ")) {
            String paramName = action.substring(13);
            LineNumberReader lineReader = new LineNumberReader(
                new StringReader(queryString));
            Set set = new HashSet();
            for (;;) {
                String s = lineReader.readLine();
                if (s == null) {
                    break;
                }
                set.add(s);
            }
            args.put(paramName, set);
            pw.println(action);
            pw.println(queryString);
            return;
        } else if (action.equals("EXPLAIN AND EXECUTE")) {
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
                    modelView,
                    query);
            } catch (Throwable ex) {
                pw.println("PREPARATION ERROR:  " + ex.getMessage());
                pw.println();
                return;
            }

            if (explain) {
                pw.println("EXPLANATION:");
                Iterator iter =
                    (new TreeMap(plan.getParamMap())).entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    pw.print("param ?");
                    pw.print(entry.getKey());
                    pw.print(" : ");
                    pw.println(entry.getValue());
                }
                plan.explain(pw);
            }

            if (execute) {
                LurqlReflectiveExecutor executor =
                    new LurqlReflectiveExecutor(
                        repos.getMdrRepos(), plan, connection,
                        args);
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
                    RefObject refObj = (RefObject) iter.next();
                    String typeName =
                        ((ModelElement) refObj.refMetaObject()).getName();
                    result.add(typeName + ": " + refObj.refGetValue("name"));
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
