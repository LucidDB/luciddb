/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.luciddb.test.udr;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.luciddb.test.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.test.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.property.*;

import com.lucidera.luciddb.test.udr.SQLRunner;


/**
 * LucidDB JDBC test for testing Remote rows UDX.
 *
 * @author Ray Zhang
 * 
 */
public class TestRemoteRowsUDX
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    public TestRemoteRowsUDX(String testname)
        throws Exception
    {
        super(testname);
    }

    //~ Methods ----------------------------------------------------------------
    
    public void setUp()
        throws Exception
    {
        // Set the properties so the LucidDB session factory and LucidDB data
        // files are used.  The LucidDB data files need to be used; otherwise,
        // we won't use versioned data segment pages.
        //
        // REVIEW zfong 7/11/08 - Is there a better way of doing this?
       FarragoProperties farragoPropInstance = FarragoProperties.instance();
        StringProperty sessionFactory =
            farragoPropInstance.defaultSessionFactoryLibraryName;
        System.setProperty(
            sessionFactory.getPath(),
            "class:org.luciddb.session.LucidDbSessionFactory");
        String homeDirString = farragoPropInstance.homeDir.get(true);
        String catalogDir = homeDirString + "/../luciddb/catalog";
        farragoPropInstance.catalogDir.set(catalogDir);
        
        // Create a new connection so we're sure we have the right session
        // factory and db
        if (connection != null) {
            connection.close();
        }
        
        connection = newConnection();
        repos = getSession().getRepos();
        saveParameters();
        
        runCleanup();
        super.setUp();
    }
    
    public static void runCleanup()
        throws Exception
    {
        // Use the special LucidDB cleanup factory to avoid dropping schemas
        // like APPLIB
        FarragoTestCase.CleanupFactory.setFactory(new LucidDbCleanupFactory());
        FarragoTestCase.runCleanup();
    }
    
    public void tearDown()
        throws Exception
    {
        // Close the connection created by this test.
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
    
    public void testRemoteRowsUDX()
        throws Exception
    {
        final String driverURI = "jdbc:luciddb:";
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();
        Properties props = newProperties();
        
        //Create  a test table.
        Connection conn = driver.connect(driverURI, props);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("create schema s");
        stmt.executeUpdate("set schema 's'");
        stmt.executeUpdate("create table t(id int, name varchar(255),is_married boolean)");
        stmt.close();
        conn.close();
        
        // Case1: Header mismatch
        conn = driver.connect(driverURI, props);
        PreparedStatement ps = conn.prepareStatement(
            "insert into s.t " +
            "select * from table( "
            + "APPLIB.REMOTE_ROWS(cursor( "
            + "select cast(null as int) as id, cast(null as varchar(255)) as name, "
            + "cast(null as boolean) as is_married " + "from (values(0)) "
            + "),7778,FALSE) " + ")");
        
        SQLRunner runner = new SQLRunner(ps);

        runner.start();
        
        Thread.sleep(5000);
        
        Socket client = new Socket("localhost", 7778);

        ObjectOutputStream objOut = new ObjectOutputStream(
            client.getOutputStream());
        List<Object> header = new ArrayList<Object>();
       
        header.add("1"); // version
        
        List format = new ArrayList();
        format.add("STRING"); // Mismatch
        format.add("STRING");
        format.add("BOOLEAN");
        
        header.add(format);
        objOut.writeObject(header);
        
        objOut.reset();
        objOut.flush();

        List<Object> list = new ArrayList<Object>();
        list.add(111);
        list.add("Test1");
        list.add(true);
        objOut.writeObject(list);
        objOut.close();
       client.close(); 
        runner.join();
        
        ps.close();
        conn.close();
               
        String errorMsg = runner.getErrorMsg();
        boolean test = false;
        if(errorMsg.indexOf("Header Info was unmatched")!=-1){
            test = true;
        }
            
        assertTrue("Header mismatch test is not passed: ",test);
        
        //Case2: Bad objects sent across (wrong datatypes in actual rows) 
        conn = driver.connect(driverURI, props);
        ps = conn.prepareStatement(
            "insert into s.t " +
            "select * from table( "
            + "APPLIB.REMOTE_ROWS(cursor( "
            + "select cast(null as int) as id, cast(null as varchar(255)) as name, "
            + "cast(null as boolean) as is_married " + "from (values(0)) "
            + "),7778,FALSE) " + ")");
        
        runner = new SQLRunner(ps);

        runner.start();
        
        Thread.sleep(5000);
        
        client = new Socket("localhost", 7778);

        objOut = new ObjectOutputStream(
            client.getOutputStream());
        header = new ArrayList<Object>();
       
        header.add("1"); // version
        
        format = new ArrayList();
        format.add("INTEGER");
        format.add("STRING");
        format.add("BOOLEAN");
        
        header.add(format);
        objOut.writeObject(header);
        
        objOut.reset();
        objOut.flush();

        list = new ArrayList<Object>();
        list.add("Test1"); // wrong type.
        list.add(111); // wrong type.
        list.add(true);
        
        objOut.writeObject(list);
        objOut.close();
       client.close(); 
        runner.join();
        
        ps.close();
        conn.close();
               
        errorMsg = runner.getErrorMsg();
        test = false;
        if(errorMsg.indexOf("Value 'Test1' cannot be converted to parameter of type INTEGER")!=-1){
            test = true;
        }
            
        assertTrue("Bad objects sent across: ",test);
        
        //Casee3: Compress stream
        conn = driver.connect(driverURI, props);
        ps = conn.prepareStatement(
            "insert into s.t " +
            "select * from table( "
            + "APPLIB.REMOTE_ROWS(cursor( "
            + "select cast(null as int) as id, cast(null as varchar(255)) as name, "
            + "cast(null as boolean) as is_married " + "from (values(0)) "
            + "),7778,TRUE) " + ")"); // TRUE
        
        runner = new SQLRunner(ps);

        runner.start();
        
        Thread.sleep(5000);
        
        client = new Socket("localhost", 7778);

        GZIPOutputStream gzOut = new GZIPOutputStream(client.getOutputStream());
        objOut = new ObjectOutputStream(
          gzOut);
        header = new ArrayList<Object>();
       
        header.add("1"); // version
        
        format = new ArrayList();
        format.add("INTEGER");
        format.add("STRING");
        format.add("BOOLEAN");
        
        header.add(format);
        objOut.writeObject(header);
        
        objOut.reset();
        objOut.flush();

        list = new ArrayList<Object>();
        list.add(111);
        list.add("Test1"); 
        list.add(true);
        
        objOut.writeObject(list);
        objOut.close();
       client.close(); 
        runner.join();
        
        ps.close();
        conn.close();
               
        errorMsg = runner.getErrorMsg();
        test = false;
        if(errorMsg == null){
            test = true;
        }
            
        assertTrue("Compress stream test is not passed: ",test);
        
        //Case4: Premature end of stream (cancel object stream) 
        //Case5: Unique constraints on server (ie, server based exception on JDBC connection)         
   
    }
    
    /**
     * Creates test connection properties.
     */
    private static Properties newProperties()
    {
        Properties props = new Properties();
        props.put("user", FarragoCatalogInit.SA_USER_NAME);
        props.put("password", "");
        return props;
    }
    
    /**
     * Cleanup factory that uses LucidDbTestCleanup
     */
    private static class LucidDbCleanupFactory
        extends FarragoTestCase.CleanupFactory
    {
        public Cleanup newCleanup(String name)
            throws Exception
        {
            return new LucidDbTestCleanup(connection);
        }
    }
}

// End LucidDbJdbcLabelTest.java
