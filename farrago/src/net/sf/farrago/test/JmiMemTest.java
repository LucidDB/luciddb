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

import net.sf.farrago.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.cwm.core.*;

import org.eigenbase.jmi.*;
import org.eigenbase.jmi.mem.*;

import junit.framework.*;

/**
 * JmiMemTest is a unit test for {@link JmiMemFactory}.
 *
 *<p>
 *
 * NOTE:  this test lives here rather than under org.eigenbase because
 * it currently depends on MDR for the metamodel JMI implementation
 * (even though JmiMemFactory itself provides a JMI implementation for the
 * model objects being tested).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiMemTest extends FarragoTestCase
{
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
    
    public static Test suite()
    {
        return wrappedSuite(JmiMemTest.class);
    }

    public void testMemFactory()
        throws Exception
    {
        FarragoMemFactory factory = new FarragoMemFactory(
            repos.getModelGraph());

        FemLocalTable table = factory.newFemLocalTable();
        FemStoredColumn column = factory.newFemStoredColumn();
        table.getFeature().add(column);
        assertSame(table, column.getOwner());
    }

    private static class FarragoMemFactory extends FarragoMetadataFactoryImpl
    {
        private final JmiMemFactory factoryImpl;

        public FarragoMemFactory(JmiModelGraph modelGraph)
        {
            factoryImpl = new FactoryImpl(modelGraph);
            this.setRootPackage((FarragoPackage) factoryImpl.getRootPackage());
        }
    }
    
    private static class FactoryImpl extends JmiModeledMemFactory
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
