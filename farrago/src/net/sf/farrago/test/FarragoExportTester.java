/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.test;

import java.io.*;
import java.util.*;
import org.netbeans.api.xmi.*;
import org.netbeans.api.mdr.*;
import org.netbeans.mdr.*;
import org.netbeans.mdr.persistence.*;
import org.netbeans.mdr.storagemodel.*;
import net.sf.farrago.util.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.*;
import javax.jmi.reflect.*;
import javax.jmi.model.*;
import javax.jmi.xmi.*;
import junit.framework.*;

/**
 * FarragoExportTester tests XMI export from the Farrago repository.
 * It doesn't run as a normal test because it has a destructive effect
 * on the repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoExportTester extends FarragoTestCase
{
    private ExportFixture exportFixture;
    
    public FarragoExportTester(String testName)
        throws Exception
    {
        super(testName);
    }

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoExportTester.class);
    }

    // implement TestCase
    public void setUp()
        throws Exception
    {
        super.setUp();

        exportFixture = new ExportFixture();
    }

    /**
     * Tests the sequence export+drop.
     */
    public void testExportAndDrop()
        throws Exception
    {
        runExport();
        runDeletion();
    }
    
    /**
     * Tests XMI export.
     */
    private void runExport()
        throws Exception
    {
        // clean out generated data
        FarragoFileAllocation dirAlloc =
            new FarragoFileAllocation(exportFixture.testdataDir);
        dirAlloc.closeAllocation();
        exportFixture.testdataDir.mkdirs();

        // perform exports
        exportXmi(
            repos.getMdrRepos(),
            exportFixture.metamodelFile,
            "FarragoMetamodel");
        exportXmi(
            repos.getMdrRepos(),
            exportFixture.catalogFile,
            "FarragoCatalog");
    }

    /**
     * Tests repository deletion.
     */
    private void runDeletion()
        throws Exception
    {
        // shut down repository
        forceShutdown();

        FarragoModelLoader modelLoader = new FarragoModelLoader();
        try {
            // grotty internals for dropping physical repos storage
            FarragoPackage farragoPackage = modelLoader.loadModel(
                "FarragoCatalog", false);
            String mofIdString = farragoPackage.refMofId();
            MOFID mofId = MOFID.fromString(mofIdString);
        
            NBMDRepositoryImpl reposImpl = (NBMDRepositoryImpl)
                modelLoader.getMdrRepos();
            Storage storage =
                reposImpl.getMdrStorage().getStorageByMofId(mofId);
            storage.close();
            storage.delete();
        } finally {
            modelLoader.close();
        }
    }
    private void exportXmi(
        MDRepository mdrRepos,
        File file,
        String extentName)
        throws Exception
    {
        RefPackage refPackage = mdrRepos.getExtent(extentName);
        XmiWriter xmiWriter = XMIWriterFactory.getDefault().createXMIWriter();
        FileOutputStream outStream = new FileOutputStream(file);
        try {
            xmiWriter.write(outStream, refPackage, "1.2");
        } finally {
            outStream.close();
        }
    }

    static class ExportFixture 
    {
        File testdataDir;
        
        File metamodelFile;
    
        File catalogFile;

        ExportFixture()
        {
            // define a private directory for generated datafiles
            String homeDir = FarragoProperties.instance().homeDir.get();
            testdataDir = new File(homeDir, "testgen");
            testdataDir = new File(testdataDir, "FarragoExportTester");
            testdataDir = new File(testdataDir, "data");
            metamodelFile = new File(testdataDir, "metamodel.xmi");
            catalogFile = new File(testdataDir, "catalog.xmi");
        }
    }
}

// End FarragoExportTester.java
