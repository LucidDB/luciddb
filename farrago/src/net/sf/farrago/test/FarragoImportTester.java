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
 * FarragoImportTester tests XMI import into the Farrago repository.  It relies
 * on {@link FarragoExportTester}, and doesn't run as a normal test because it
 * has a destructive effect on the repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoImportTester extends TestCase
{
    private FarragoExportTester.ExportFixture exportFixture;
    
    public FarragoImportTester(String testName)
        throws Exception
    {
        super(testName);
    }
    
    // implement TestCase
    public void setUp()
        throws Exception
    {
        super.setUp();

        exportFixture = new FarragoExportTester.ExportFixture();
    }

    /**
     * Tests the import.
     */
    public void testImport()
        throws Exception
    {
        runImport();
    }
    
    /**
     * Tests XMI import (depends on testExport and testDeletion running first).
     */
    private void runImport()
        throws Exception
    {
        FarragoModelLoader modelLoader = new FarragoModelLoader();
        modelLoader = new FarragoModelLoader();
        try {
            modelLoader.initStorage(false);

            // import metamodel
            importXmi(
                modelLoader.getMdrRepos(),
                exportFixture.metamodelFile,
                "FarragoMetamodel",
                null,
                null);
            
            // import catalog
            importXmi(
                modelLoader.getMdrRepos(),
                exportFixture.catalogFile,
                "FarragoCatalog",
                "FarragoMetamodel",
                "Farrago");
            
        } finally {
            modelLoader.close();
        }
    }


    private void importXmi(
        MDRepository mdrRepos,
        File file,
        String extentName,
        String metaPackageExtentName,
        String metaPackageName)
        throws Exception
    {
        RefPackage extent;
        if (metaPackageExtentName != null) {
            ModelPackage modelPackage = (ModelPackage)
                mdrRepos.getExtent(metaPackageExtentName);
            MofPackage metaPackage = null;
            Iterator iter =
                modelPackage.getMofPackage().refAllOfClass().iterator();
            while (iter.hasNext()) {
                MofPackage result = (MofPackage) iter.next();
                if (result.getName().equals(metaPackageName)) {
                    metaPackage = result;
                    break;
                }
            }
            extent = mdrRepos.createExtent(extentName, metaPackage);
        } else {
            extent = mdrRepos.createExtent(extentName);
        }
        XmiReader xmiReader = XMIReaderFactory.getDefault().createXMIReader();
        boolean rollback = false;
        try {
            mdrRepos.beginTrans(true);
            rollback = true;
            xmiReader.read(file.toURL().toString(), extent);
            rollback = false;
            mdrRepos.endTrans();
        } finally {
            if (rollback) {
                mdrRepos.endTrans(true);
            }
        }
    }
}

// End FarragoImportTester.java
