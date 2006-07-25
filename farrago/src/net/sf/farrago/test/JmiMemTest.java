/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import javax.jmi.reflect.*;

import junit.framework.*;

import net.sf.farrago.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.jmi.*;
import org.eigenbase.jmi.mem.*;

import org.netbeans.api.xmi.*;


/**
 * JmiMemTest is a unit test for {@link JmiMemFactory}.
 *
 * <p>NOTE: this test lives here rather than under org.eigenbase because it
 * currently depends on MDR for the metamodel JMI implementation (even though
 * JmiMemFactory itself provides a JMI implementation for the model objects
 * being tested).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiMemTest
    extends FarragoTestCase
{

    //~ Static fields/initializers ---------------------------------------------

    private static final String TABLE_NAME = "Chips Ahoy";

    private static final String COLUMN_NAME = "Keebler Elves";

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new JmiMemTest object.
     *
     * @param testName JUnit test name
     */
    public JmiMemTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    public static Test suite()
    {
        return wrappedSuite(JmiMemTest.class);
    }

    public void testEarlyBinding()
        throws Exception
    {
        FarragoMemFactory factory =
            new FarragoMemFactory(
                repos.getModelGraph());

        FemLocalTable table = factory.newFemLocalTable();
        table.setName(TABLE_NAME);
        FemStoredColumn column = factory.newFemStoredColumn();
        table.getFeature().add(column);
        assertSame(
            table,
            column.getOwner());
        assertSame(
            table,
            column.refImmediateComposite());
        assertEquals(
            TABLE_NAME,
            table.getName());

        assertNull(factory.getImpl().getPersistentMofId(table));
        factory.getImpl().setPersistentMofId(table, "XYZZY");
        assertEquals(
            "XYZZY",
            factory.getImpl().getPersistentMofId(table));

        RefClass tableClass = table.refClass();
        RefObject tableObj =
            tableClass.refCreateInstance(
                Collections.singletonList(TABLE_NAME));
        assertTrue(tableObj instanceof FemLocalTable);
        table = (FemLocalTable) tableObj;
        assertEquals(
            TABLE_NAME,
            table.getName());
    }

    public void testBreakOneToOneAssoc()
        throws Exception
    {
        FarragoMemFactory factory =
            new FarragoMemFactory(
                repos.getModelGraph());
        FemStoredColumn col = factory.newFemStoredColumn();
        FemSequenceGenerator seq = factory.newFemSequenceGenerator();
        col.setSequence(seq);

        assertSame(
            col,
            seq.getColumn());

        col.setSequence(null);

        assertNull(seq.getColumn());
    }

    public void testExportImport()
    {
        FarragoMemFactory factory =
            new FarragoMemFactory(
                repos.getModelGraph());

        FemLocalTable table = factory.newFemLocalTable();
        table.setName(TABLE_NAME);
        FemStoredColumn column = factory.newFemStoredColumn();
        column.setName(COLUMN_NAME);
        table.getFeature().add(column);

        String xmi = JmiObjUtil.exportToXmiString(
                Collections.singleton(table));

        Collection c =
            JmiObjUtil.importFromXmiString(
                factory.getImpl().getRootPackage(),
                xmi);

        assertEquals(
            1,
            c.size());

        Object root = c.iterator().next();
        assertTrue(root instanceof FemLocalTable);
        table = (FemLocalTable) root;
        assertEquals(
            TABLE_NAME,
            table.getName());

        c = table.getFeature();
        assertEquals(
            1,
            c.size());
        Object child = c.iterator().next();
        assertTrue(child instanceof FemStoredColumn);
        column = (FemStoredColumn) child;
        assertEquals(
            COLUMN_NAME,
            column.getName());
    }

    public void testMassiveExportImport()
        throws Exception
    {
        // First, export the entire repository from MDR storage.
        XMIWriter xmiWriter = XMIWriterFactory.getDefault().createXMIWriter();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();

        // While exporting, hide FemFennelConfig, because JmiMemFactory
        // can't currently handle the 1-to-1 association
        // FarragoConfiguresFennel.
        repos.beginReposTxn(true);
        try {
            repos.getCurrentConfig().getFennelConfig().refDelete();
            xmiWriter.write(
                outStream,
                repos.getFarragoPackage(),
                "1.2");
        } finally {
            // rollback
            repos.endReposTxn(true);
        }
        String xmi1 = outStream.toString();

        // Import into an in-mem repository.
        FarragoMemFactory factory =
            new FarragoMemFactory(
                repos.getModelGraph());
        Collection c =
            JmiObjUtil.importFromXmiString(
                factory.getImpl().getRootPackage(),
                xmi1);

        // Re-export from there.  This time we use the root objects from
        // the import because JmiMemFactory has no notion of
        // extents.
        outStream = new ByteArrayOutputStream();
        xmiWriter.write(outStream, c, "1.2");
        String xmi2 = outStream.toString();

        // Now diff:  thanks to the way default XMI id generation works,
        // the XMI content should come out the same.
        xmi1 = xmi1.replaceFirst("timestamp = \'.*\'", "timestamp= XXX");
        xmi2 = xmi2.replaceFirst("timestamp = \'.*\'", "timestamp= XXX");
        assertEquals(xmi1, xmi2);
    }

    //~ Inner Classes ----------------------------------------------------------

    private class FarragoMemFactory
        extends FarragoMetadataFactoryImpl
    {
        private final FactoryImpl factoryImpl;

        public FarragoMemFactory(JmiModelGraph modelGraph)
        {
            factoryImpl = new FactoryImpl(modelGraph);
            this.setRootPackage((FarragoPackage) factoryImpl.getRootPackage());
        }

        public FactoryImpl getImpl()
        {
            return factoryImpl;
        }
    }

    private class FactoryImpl
        extends JmiModeledMemFactory
    {
        FactoryImpl(JmiModelGraph modelGraph)
        {
            super(modelGraph);
        }

        protected RefPackageImpl newRootPackage()
        {
            return new RefPackageImpl(FarragoPackage.class);
        }
    }
}

// End JmiMemTest.java
