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
import org.netbeans.api.xmi.*;
import net.sf.farrago.util.*;
import javax.jmi.reflect.*;

/**
 * FarragoReposTest tests the Farrago repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoReposTest extends FarragoTestCase
{
    public FarragoReposTest(String testName)
        throws Exception
    {
        super(testName);
    }
    
    /**
     * Tests XMI export and import of catalog.
     */
    public void testExportImport()
        throws Exception
    {
        // create a private directory for generated datafiles
        String homeDir = FarragoProperties.instance().homeDir.get();
        File testdataDir = new File(homeDir, "testgen");
        testdataDir = new File(testdataDir, "FarragoReposTest");
        testdataDir = new File(testdataDir, "data");
        FarragoFileAllocation dirAlloc =
            new FarragoFileAllocation(testdataDir);
        dirAlloc.closeAllocation();
        testdataDir.mkdirs();

        // perform export
        File file = new File(testdataDir, "export.xmi");
        XMIWriter xmiWriter = XMIWriterFactory.getDefault().createXMIWriter();
        FileOutputStream outStream = new FileOutputStream(file);
        try {
            xmiWriter.write(outStream, repos.getFarragoPackage(), "1.2");
        } catch (JmiException ex) {
            System.err.println("ELEMENT:  " + ex.getElementInError());
            System.err.println("OBJECT:  " + ex.getObjectInError());
            throw ex;
        } finally {
            outStream.close();
        }
    }
}

// End FarragoReposTest.java
