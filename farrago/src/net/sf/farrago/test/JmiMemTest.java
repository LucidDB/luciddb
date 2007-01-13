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
import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.CwmModelElement;
import net.sf.farrago.cwm.core.CwmTaggedValue;
import net.sf.farrago.cwm.relational.CwmCatalog;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.config.FemFarragoConfig;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.tuple.FennelStandardTypeDescriptor;
import net.sf.farrago.query.FennelRelUtil;
import net.sf.farrago.type.FarragoTypeFactoryImpl;

import org.eigenbase.jmi.*;
import org.eigenbase.jmi.mem.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.SaffronProperties;

import org.netbeans.api.mdr.MDRepository;
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
    private static final String COLUMN_NAME_2 = "Cap'n Crunch";

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

    public void testClassValuedAttributes()
    {
        FarragoMemRepos factory = new FarragoMemRepos(repos.getModelGraph());

        FemTableInserterDef inserterDef = factory.newFemTableInserterDef();
        
        inserterDef.setName(TABLE_NAME);
        
        FemTupleDescriptor tupleDesc = makeTupleDescriptor(factory);
        inserterDef.setOutputDesc(tupleDesc);

        FemIndexWriterDef indexWriterDef = factory.newFemIndexWriterDef();
        indexWriterDef.setIndexId(999L);
        
        inserterDef.getIndexWriter().add(indexWriterDef);

        // NOTE: SWZ: 12/1/2006: Changed to use addAll to mimic 
        // FarragoPreparingStmt's actual behavior.
        FemCmdPrepareExecutionStreamGraph cmd =
            factory.newFemCmdPrepareExecutionStreamGraph();
        cmd.getStreamDefs().addAll(
            Collections.singleton(inserterDef));
        
        String xmi = JmiObjUtil.exportToXmiString(
                Collections.singleton(cmd));

        Collection c =
            JmiObjUtil.importFromXmiString(
                factory.getImpl().getRootPackage(),
                xmi);
        assertEquals(
            1,
            c.size());

        cmd = (FemCmdPrepareExecutionStreamGraph) c.iterator().next();
        assertEquals(
            1,
            cmd.getStreamDefs().size());

        inserterDef = 
            (FemTableInserterDef) cmd.getStreamDefs().iterator().next();
        assertEquals(
            1,
            inserterDef.getIndexWriter().size());

        indexWriterDef = 
            (FemIndexWriterDef) inserterDef.getIndexWriter().iterator().next();
        assertEquals(
            999L,
            indexWriterDef.getIndexId());
        
        tupleDesc = (FemTupleDescriptor) inserterDef.getOutputDesc();
        assertNotNull(tupleDesc);
        
        List<FemTupleAttrDescriptor> attrDescs = tupleDesc.getAttrDescriptor();
        assertEquals(2, attrDescs.size());
        
        FemTupleAttrDescriptor col1 = attrDescs.get(0);
        assertEquals(
            FennelStandardTypeDescriptor.INT_32_ORDINAL,
            col1.getTypeOrdinal());
        
        FemTupleAttrDescriptor col2 = attrDescs.get(1);
        assertEquals(
            FennelStandardTypeDescriptor.VARCHAR_ORDINAL,
            col2.getTypeOrdinal());
        assertEquals(128, col2.getByteLength());
    }

    private FemTupleDescriptor makeTupleDescriptor(FarragoMemRepos factory)
    {
        RelDataTypeFactory typeFactory = new FarragoTypeFactoryImpl(factory);

        RelDataType[] types = new RelDataType[] {
            typeFactory.createSqlType(SqlTypeName.Integer),
            typeFactory.createSqlType(SqlTypeName.Varchar, 128),
        };
        
        String[] names = new String[] {
            COLUMN_NAME,
            COLUMN_NAME_2,
        };
        
        RelDataType rowType = typeFactory.createStructType(types, names);
        
        FemTupleDescriptor tupleDesc = 
            FennelRelUtil.createTupleDescriptorFromRowType(
                factory,
                typeFactory,
                rowType);
        return tupleDesc;
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

        FarragoReposTxnContext txn = repos.newTxnContext();
        try {
            txn.beginReadTxn();
            xmiWriter.write(
                outStream,
                repos.getFarragoPackage(),
                "1.2");
        } finally {
            txn.commit();
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

    public void testRefImmediatePackage()
    {
        FarragoMemFactory factory =
            new FarragoMemFactory(
                repos.getModelGraph());
        FemStoredColumn col = factory.newFemStoredColumn();
        
        RefPackage pkg = col.refImmediatePackage();
        RefPackage expectedPkg = factory.getMedPackage();
        assertEquals(expectedPkg, pkg);
        
        // Traverse our way up to the root package
        pkg = pkg.refImmediatePackage();
        expectedPkg = factory.getFemPackage();
        assertEquals(expectedPkg, pkg);
        
        pkg = pkg.refImmediatePackage();
        expectedPkg = factory.getFarragoPackage();
        assertEquals(expectedPkg, pkg);

        // Farrago is the root package.
        pkg = pkg.refImmediatePackage();
        assertNull(pkg);
    }
    
    public void testRefIsInstanceOf()
    {
        FarragoMemFactory factory =
            new FarragoMemFactory(
                repos.getModelGraph());

        FemLocalTable table = factory.newFemLocalTable();
        table.setName(TABLE_NAME);

        RefObject cwmTableMofClass = 
            factory.getRelationalPackage().getCwmTable().refMetaObject();

        RefObject femLocalTableClass =
            factory.getMedPackage().getFemLocalTable().refMetaObject();
        
        // table is a FemLocalTable which is a subclass of CwmTable 
        assertTrue(table.refIsInstanceOf(cwmTableMofClass, true));
        
        // table is a FemLocalTale which is not exactly CwmTable
        assertTrue(!table.refIsInstanceOf(cwmTableMofClass, false));
        
        assertTrue(table.refIsInstanceOf(femLocalTableClass, true));
        assertTrue(table.refIsInstanceOf(femLocalTableClass, false));        
    }
    
    //~ Inner Classes ----------------------------------------------------------

    private class FarragoMemRepos
        extends FarragoMemFactory
        implements FarragoRepos
    {
        public FarragoMemRepos(JmiModelGraph modelGraph)
        {
            super(modelGraph);
        }

        public MDRepository getMdrRepos()
        {
            throw new UnsupportedOperationException();
        }

        public JmiModelGraph getModelGraph()
        {
            return getImpl().getJmiModelGraph(); 
        }

        public JmiModelView getModelView()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoPackage getTransientFarragoPackage()
        {
            return (FarragoPackage)getImpl().getRootPackage();
        }

        public CwmCatalog getSelfAsCatalog()
        {
            throw new UnsupportedOperationException();
        }

        public int getIdentifierPrecision()
        {
            throw new UnsupportedOperationException();
        }

        public FemFarragoConfig getCurrentConfig()
        {
            throw new UnsupportedOperationException();
        }

        public String getDefaultCharsetName()
        {
            return SaffronProperties.instance().defaultCharset.get();
        }

        public String getDefaultCollationName()
        {
            return SaffronProperties.instance().defaultCollation.get();
        }

        public boolean isFennelEnabled()
        {
            return true;
        }

        public String getLocalizedObjectName(CwmModelElement modelElement)
        {
            throw new UnsupportedOperationException();
        }

        public String getLocalizedObjectName(String name)
        {
            throw new UnsupportedOperationException();
        }

        public String getLocalizedObjectName(CwmModelElement modelElement, RefClass refClass)
        {
            throw new UnsupportedOperationException();
        }

        public String getLocalizedObjectName(String qualifierName, String objectName, RefClass refClass)
        {
            throw new UnsupportedOperationException();
        }

        public String getLocalizedClassName(RefClass refClass)
        {
            throw new UnsupportedOperationException();
        }

        public CwmCatalog getCatalog(String catalogName)
        {
            throw new UnsupportedOperationException();
        }

        public CwmTaggedValue getTag(CwmModelElement element, String tagName)
        {
            throw new UnsupportedOperationException();
        }

        public void setTagValue(CwmModelElement element, String tagName, String tagValue)
        {
            throw new UnsupportedOperationException();
        }

        public String getTagValue(CwmModelElement element, String tagName)
        {
            throw new UnsupportedOperationException();
        }

        public FemTagAnnotation getTagAnnotation(FemAnnotatedElement element, String tagName)
        {
            throw new UnsupportedOperationException();
        }

        public void setTagAnnotationValue(FemAnnotatedElement element, String tagName, String tagValue)
        {
            throw new UnsupportedOperationException();            
        }

        public String getTagAnnotationValue(FemAnnotatedElement element, String tagName)
        {
            throw new UnsupportedOperationException();
        }

        public void addResourceBundles(List bundles)
        {
            throw new UnsupportedOperationException();
        }

        public void beginReposTxn(boolean writable)
        {
            throw new UnsupportedOperationException();
        }

        public void endReposTxn(boolean rollback)
        {
            throw new UnsupportedOperationException();
        }

        public Object getMetadataFactory(String prefix)
        {
            return getImpl();
        }

        public FarragoSequenceAccessor getSequenceAccessor(String mofId)
        {
            throw new UnsupportedOperationException();
        }

        public String expandProperties(String value)
        {
            throw new UnsupportedOperationException();
        }

        public <T extends RefObject> Collection<T> allOfClass(Class<T> clazz)
        {
            throw new UnsupportedOperationException();
        }

        public <T extends RefObject> Collection<T> allOfType(Class<T> clazz)
        {
            throw new UnsupportedOperationException();
        }

        public List<FarragoReposIntegrityErr> verifyIntegrity(RefObject refObj)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoModelLoader getModelLoader()
        {
            throw new UnsupportedOperationException();
        }

        public void closeAllocation()
        {
        }
    
        // implement FarragoRepos
        public FarragoReposTxnContext newTxnContext()
        {
            return new FarragoReposTxnContext(this);
        }
    }
    
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
        
        public JmiModelGraph getJmiModelGraph()
        {
            return getModelGraph();
        }
    }
}

// End JmiMemTest.java
