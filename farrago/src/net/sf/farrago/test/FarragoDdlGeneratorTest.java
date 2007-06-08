/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.*;

import junit.framework.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.ddl.gen.*;
import net.sf.farrago.fem.med.*;

import org.eigenbase.test.*;


/**
 * Unit test cases for {@link FarragoDdlGenerator}.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class FarragoDdlGeneratorTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    public FarragoDdlGeneratorTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    protected DdlGenerator newDdlGenerator()
    {
        return new FarragoDdlGenerator();
    }

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(FarragoDdlGeneratorTest.class);
    }

    public static Test suite()
    {
        return wrappedSuite(FarragoDdlGeneratorTest.class);
    }

    public void testExportSales()
    {
        String output = exportSchema("SALES", true);
        getDiffRepos().assertEquals("output", "${output}", output);
    }

    private String exportSchema(
        String schemaName,
        boolean includeNonSchemaElements)
    {
        DdlGenerator ddlGen = newDdlGenerator();
        List<CwmModelElement> list = new ArrayList<CwmModelElement>();
        gatherElements(list, schemaName, includeNonSchemaElements);
        return ddlGen.getExportText(list);
    }

    /**
     * Gathers a list of elements in a schema, optionally including elements
     * which don't belong to any schema.
     *
     * @param list List to populate
     * @param schemaName Name of schema
     * @param includeNonSchemaElements Whether to include elements which do not
     * belong to a schema
     */
    protected void gatherElements(
        List<CwmModelElement> list,
        String schemaName,
        boolean includeNonSchemaElements)
    {
        CwmCatalog catalog = repos.getSelfAsCatalog();
        for (CwmModelElement element : catalog.getOwnedElement()) {
            if (element instanceof CwmSchema) {
                CwmSchema schema = (CwmSchema) element;
                if (schema.getName().equals(schemaName)) {
                    list.add(schema);
                    for (CwmModelElement element2 : schema.getOwnedElement()) {
                        list.add(element2);
                    }
                }
            } else if (includeNonSchemaElements) {
                list.add(element);
            }
        }
        if (includeNonSchemaElements) {
            for (
                FemDataServer dataServer : repos.allOfType(FemDataServer.class))
            {
                list.add(dataServer);
            }
            for (
                FemDataWrapper dataWrapper
                : repos.allOfType(FemDataWrapper.class))
            {
                list.add(dataWrapper);
            }
        }
    }
    
    /**
     * Test DDL generation for objects that don't have all the optional
     * clauses.
     */
    public void testDdlGeneration()
    {
        StringBuilder output = new StringBuilder();
        
        // Create a DDL Generator for this test
        DdlGenerator ddlGenerator = newDdlGenerator();
        
        // Set up objects that do not include optional items
        FemDataWrapper wrapper = repos.newFemDataWrapper();
        wrapper.setName("TESTWRAPPER");
        wrapper.setLanguage("JAVA");
        FemDataServer server = repos.newFemDataServer();
        server.setName("TESTSERVER");
        server.setWrapper(wrapper);
        
        // Generate DDL for minimal objects
        GeneratedDdlStmt stmt = new GeneratedDdlStmt();
        ddlGenerator.generateCreate(wrapper, stmt);
        appendStatementText(output, stmt);
        stmt.clear();
        ddlGenerator.generateCreate(server, stmt);
        appendStatementText(output, stmt);
        stmt.clear();
        
        // add an optional element
        wrapper.setLibraryFile("net.sf.farrago.TestWrapper");
        ddlGenerator.generateCreate(wrapper, stmt);
        appendStatementText(output, stmt);
        stmt.clear();
        
        server.setType("TESTTYPE");
        ddlGenerator.generateCreate(server, stmt);
        appendStatementText(output, stmt);
        stmt.clear();
        
        server.setVersion("TESTVERSION");
        ddlGenerator.generateCreate(server, stmt);
        appendStatementText(output, stmt);
        stmt.clear();
        
        getDiffRepos().assertEquals("output", "${output}", output.toString());
    }

    /**
     * Appends all the statements in a {@link GeneratedDdlStmt} object to the
     * end of a string.
     * @param sb StringBuilder object to hold the text
     * @param stmt GeneratedDdlStmt object we want the text for
     */
    private void appendStatementText(StringBuilder sb, GeneratedDdlStmt stmt)
    {
        for (String s : stmt.getStatementList()) {
            sb.append(s);
        }
        sb.append("\n\n");
    }
}

// End FarragoDdlGeneratorTest.java
