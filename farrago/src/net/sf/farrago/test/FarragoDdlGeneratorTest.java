/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2008 The Eigenbase Project
// Copyright (C) 2005-2008 Disruptive Tech
// Copyright (C) 2005-2008 LucidEra, Inc.
// Portions Copyright (C) 2003-2008 John V. Sichi
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

import java.util.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.ddl.gen.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.test.*;
import org.eigenbase.util.*;


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
        return new FarragoDdlGenerator(repos.getModelView());
    }

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(FarragoDdlGeneratorTest.class);
    }

    public static Test suite()
    {
        return wrappedSuite(FarragoDdlGeneratorTest.class);
    }

    /* (non-Javadoc)
     * @see net.sf.farrago.test.FarragoTestCase#setUp()
     */
    @Override protected void setUp()
        throws Exception
    {
        runCleanup();
        super.setUp();
    }

    public void testExportSales()
    {
        String output = exportSchema("SALES", true);

        // REVIEW: SWZ: 2008-10-07: Output varies based on repository
        // configuration.  Handle the variance by repository type.  When
        // all repositories switch to Enki/Hibernate we can remove the
        // else case and conditional.
        MdrProvider providerType = repos.getEnkiMdrRepos().getProviderType();
        if (providerType == MdrProvider.ENKI_HIBERNATE) {
            getDiffRepos().assertEquals(
                "output-hibernate",
                "${output-hibernate}",
                output);
        } else {
            getDiffRepos().assertEquals("output", "${output}", output);
        }
    }

    private String exportSchema(
        String schemaName,
        boolean includeNonSchemaElements)
    {
        repos.beginReposSession();
        repos.beginReposTxn(false);

        try {
            DdlGenerator ddlGen = newDdlGenerator();
            List<CwmModelElement> list = new ArrayList<CwmModelElement>();
            ddlGen.gatherElements(
                list,
                schemaName,
                includeNonSchemaElements,
                repos.getSelfAsCatalog());
            return ddlGen.getExportText(list, true);
        } finally {
            repos.endReposTxn(false);
            repos.endReposSession();
        }
    }

    /**
     * Test DDL generation for objects that don't have all the optional clauses.
     */
    public void testDdlGeneration()
    {
        StringBuilder output = new StringBuilder();

        // Create a DDL Generator for this test
        DdlGenerator ddlGenerator = newDdlGenerator();

        FarragoReposTxnContext reposTxnContext =
            new FarragoReposTxnContext(repos, true);
        reposTxnContext.beginWriteTxn();

        try {
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

            // now drop 'em
            ddlGenerator.generateDrop(server, stmt);
            appendStatementText(output, stmt);
            stmt.clear();

            ddlGenerator.generateDrop(wrapper, stmt);
            appendStatementText(output, stmt);
            stmt.clear();
        } finally {
            reposTxnContext.rollback();
        }

        getDiffRepos().assertEquals("output", "${output}", output.toString());
    }

    public void testCascade()
        throws Exception
    {
        final String SCHEMA_NAME = "CASCADE_TEST";
        final String TABLE_NAME = "FOO";
        final String COLUMN_NAME = "A";
        final String VIEW_NAME = "BAR";
        StringBuilder output = new StringBuilder();

        FarragoReposTxnContext reposTxnContext =
            new FarragoReposTxnContext(repos, true);
        reposTxnContext.beginWriteTxn();

        final File tempFile = File.createTempFile("cascade", ".sql");
        try {
            // Create a DDL Generator for this test
            DdlGenerator ddlGenerator = newDdlGenerator();

            // create a SCHEMA
            FemLocalSchema schema = repos.newFemLocalSchema();
            schema.setName(SCHEMA_NAME);
            schema.setVisibility(VisibilityKindEnum.VK_PUBLIC);
            schema.setNamespace(repos.getCatalog("LOCALDB"));

            // create a simple TABLE
            FemLocalTable table = repos.newFemLocalTable();
            table.setName(TABLE_NAME);
            table.setNamespace(schema);
            table.setDescription("Test");
            table.setVisibility(VisibilityKindEnum.VK_PUBLIC);
            FemStoredColumn column = repos.newFemStoredColumn();
            column.setName(COLUMN_NAME);
            table.getFeature().add(column);

            // create a VIEW off the TABLE
            FemLocalView view = repos.newFemLocalView();
            view.setName(VIEW_NAME);
            view.setNamespace(schema);
            view.setVisibility(VisibilityKindEnum.VK_PUBLIC);
            CwmQueryExpression query = repos.newCwmQueryExpression();
            query.setBody("SELECT * FROM " + TABLE_NAME);
            view.setQueryExpression(query);

            // drop the SCHEMA (with CASCADE)
            GeneratedDdlStmt stmt = new GeneratedDdlStmt();
            ddlGenerator.setDropCascade(true);
            ddlGenerator.generateDrop(schema, stmt);
            appendStatementText(output, stmt);

            // save the statement for cleanup use
            final FileWriter fw = new FileWriter(tempFile);
            fw.write(output.toString() + ";" + TestUtil.NL);
            fw.close();
            stmt.clear();

            // drop the SCHEMA (without CASCADE)
            ddlGenerator.setDropCascade(false);
            ddlGenerator.generateDrop(schema, stmt);
            appendStatementText(output, stmt);
            stmt.clear();

            // drop the table (with CASCADE)
            ddlGenerator.setDropCascade(true);
            ddlGenerator.generateDrop(table, stmt);
            appendStatementText(output, stmt);
            stmt.clear();

            // drop the table (without CASCADE)
            ddlGenerator.setDropCascade(false);
            ddlGenerator.generateDrop(table, stmt);
            appendStatementText(output, stmt);
            stmt.clear();

            // drop the view (with CASCADE)
            ddlGenerator.setDropCascade(true);
            ddlGenerator.generateDrop(view, stmt);
            appendStatementText(output, stmt);
            stmt.clear();

            // drop the view (without CASCADE)
            ddlGenerator.setDropCascade(false);
            ddlGenerator.generateDrop(view, stmt);
            appendStatementText(output, stmt);
            stmt.clear();
        } finally {
            reposTxnContext.rollback();
        }

        getDiffRepos().assertEquals("output", "${output}", output.toString());

        // clean up afterward
        runSqlLineTest(tempFile.getAbsolutePath(), false);

        // If successful, delete temp file and log
        tempFile.delete();
        new File(tempFile.getAbsolutePath().replace(".sql", ".log")).delete();
    }

    /**
     * Appends all the statements in a {@link GeneratedDdlStmt} object to the
     * end of a string.
     *
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

    public void _testCustomSchema()
        throws Exception
    {
        // Run the script to create the objects.
        runSqlLineTest("unitsql/ddl/ddlgen.sql");

        // Export the schema and compare with expected output.
        String output = exportSchema("DDLGEN", true);
        final String guidRegex =
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
        String outputMasked = output.replaceAll(guidRegex, "{guid}");
        getDiffRepos().assertEquals("output", "${output}", outputMasked);

        // Write to temp file and run it, to make sure it is valid SQL. If the
        // DDL is invalid, we will find out next step.
        final File tempFile = File.createTempFile("allTypes", ".sql");
        final FileWriter fw = new FileWriter(tempFile);
        fw.write("DROP SCHEMA ddlgen CASCADE;");
        fw.write(TestUtil.NL);
        fw.write("SET SCHEMA 'ddlgen';");
        fw.write(TestUtil.NL);
        fw.write("SET PATH 'ddlgen';");
        fw.write(TestUtil.NL);
        fw.write(output);
        fw.close();
        runSqlLineTest(tempFile.getAbsolutePath(), false);

        // Export the schema again and make sure the output is the same as last
        // time.
        String output2 = exportSchema("DDLGEN", true);
        String output2Masked = output2.replaceAll(guidRegex, "{guid}");
        TestUtil.assertEqualsVerbose(outputMasked, output2Masked);

        // If successful, delete temp file and log
        tempFile.delete();
        new File(tempFile.getAbsolutePath().replace(".sql", ".log")).delete();
    }
}

// End FarragoDdlGeneratorTest.java
