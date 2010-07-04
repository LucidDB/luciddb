/*
// $Id$
// SFDC Connector is an Eigenbase SQL/MED connector for Salesforce.com
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */
package net.sf.farrago.namespace.sfdc.test;

import com.sforce.soap.enterprise.*;
import com.sforce.soap.enterprise.fault.*;
import com.sforce.soap.enterprise.sobject.*;

import java.io.*;
import java.rmi.RemoteException;
import java.text.*;
import java.util.*;
import javax.xml.rpc.ServiceException;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

/**
 * Creates, then deletes 2 SFDC Account objects.
 * This is basically a helper class for the deletes.sql test and
 * incrementalExport.sql test
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class CreateAndDeleteTest extends TestCase
{

    private SoapBindingStub binding;
    String[] ids = new String[2];
    private String username = System.getenv("username");
    private String passwd = System.getenv("password");

    public CreateAndDeleteTest(String method)
    {
        super(method);
    }

    public void testCreateDelete()
        throws Exception
    {
        binding = (SoapBindingStub) new SforceServiceLocator().getSoap();

        // timeout after 1 min
        binding.setTimeout(60000);
        String platformHome1 = System.getenv("SFDC_TEST_HOME");
        LoginResult loginResult = binding.login(username, passwd);
        binding._setProperty(
            SoapBindingStub.ENDPOINT_ADDRESS_PROPERTY,
            loginResult.getServerUrl());
        // Create a new session header object and set the session id to that
        // returned by the login
        SessionHeader sh = new SessionHeader();
        sh.setSessionId(loginResult.getSessionId());
        binding.setHeader(
            new SforceServiceLocator().getServiceName().getNamespaceURI(),
            "SessionHeader",
            sh);

        Calendar cal = binding.getServerTimestamp().getTimestamp();
        int month = (cal.get(Calendar.MONTH)+1) % 13;
        String startTime =
            cal.get(Calendar.YEAR) + "-"
            + month + "-"
            + cal.get(Calendar.DAY_OF_MONTH)
            + " "
            + cal.get(Calendar.HOUR_OF_DAY) + ":"
            + cal.get(Calendar.MINUTE) + ":"
            + cal.get(Calendar.SECOND);

        createAccountSample();
        deleteSample();

        // sleep for 1+ minutes to guarantee a time difference
        Thread.sleep(100000);
        cal = binding.getServerTimestamp().getTimestamp();
        month = (cal.get(Calendar.MONTH)+1) % 13;
        String endTime =
            cal.get(Calendar.YEAR) + "-"
            + month + "-"
            + cal.get(Calendar.DAY_OF_MONTH)
            + " "
            + cal.get(Calendar.HOUR_OF_DAY) + ":"
            + cal.get(Calendar.MINUTE) + ":"
            + cal.get(Calendar.SECOND);

        // write the start and end times to file
        String platformHome = System.getenv("SFDC_TEST_HOME");
        char sep = File.separatorChar;
        String filePath = "tmp.map";
        File file = new File(platformHome, filePath);
        FileWriter writer = new FileWriter(file, false);
        writer.write("@S_TIME@=" + startTime + "\r\n");
        writer.write("@E_TIME@=" + endTime + "\r\n");
        writer.close();
    }

    public void testCreateRows()
        throws Exception
    {
        binding = (SoapBindingStub) new SforceServiceLocator().getSoap();

        // timeout after 1 min
        binding.setTimeout(60000);
        LoginResult loginResult = binding.login(username, passwd);
        binding._setProperty(
            SoapBindingStub.ENDPOINT_ADDRESS_PROPERTY,
            loginResult.getServerUrl());
        // Create a new session header object and set the session id to that
        // returned by the login
        SessionHeader sh = new SessionHeader();
        sh.setSessionId(loginResult.getSessionId());
        binding.setHeader(
            new SforceServiceLocator().getServiceName().getNamespaceURI(),
            "SessionHeader",
            sh);

        // Gets last export time
        Calendar cal = binding.getServerTimestamp().getTimestamp();
        int month = (cal.get(Calendar.MONTH)+1) % 13;
        String startTime =
            cal.get(Calendar.YEAR) + "-"
            + month + "-"
            + cal.get(Calendar.DAY_OF_MONTH)
            + " "
            + cal.get(Calendar.HOUR_OF_DAY) + ":"
            + cal.get(Calendar.MINUTE) + ":"
            + cal.get(Calendar.SECOND);

        // Create two account objects
        Account account1 = new Account();
        Account account2 = new Account();

        // Set some fields on the account object
        // Name field (required) not being set on account1,
        // so this record should fail during create.
        account1.setName("Bogus Account");
        account1.setBillingCity("Bogus City");
        account1.setBillingCountry("Bogus Country");
        account1.setBillingState("Bogus State");
        account1.setBillingStreet("0000 Bogus Lane");
        account1.setBillingPostalCode("00000");

        // Set some fields on the account2 object
        account2.setName("Mimi Pet Shop");
        account2.setBillingCity("Oakland");
        account2.setBillingCountry("US");
        account2.setBillingState("CA");
        account2.setBillingStreet("7421 Park St.");
        account2.setBillingPostalCode("97502");

        // Create an array of SObjects to hold the accounts
        SObject[] sObjects = new SObject[2];
        // Add the accounts to the SObject array
        sObjects[0] = account1;
        sObjects[1] = account2;

        // Invoke the create call
        SaveResult[] saveResults = binding.create(sObjects);

        String platformHome = System.getenv("SFDC_TEST_HOME");
        char sep = File.separatorChar;
        String filePath = "";
        File file = new File(platformHome, filePath+"ids");
        if (file.exists()) {
            // accounts from previous runs may exist, cleanup
            testDeleteRows();
        }
        FileWriter writer = new FileWriter(file, false);

        // Handle the results
        for (int i=0; i<saveResults.length; i++) {
            // Determine whether create succeeded or had errors
            if (saveResults[i].isSuccess()) {
                // save ids to a file
                writer.write(saveResults[i].getId()+"\n");
            } else {
                // Cleans up and handles the errors
                file.delete();
                writer.close();
                throw new junit.framework.AssertionFailedError(
                    "Error creating objects");
            }
        }
        writer.close();

        // writes last export time and username/password to file
        file = new File(platformHome, filePath+"exporttmp.map");
        writer = new FileWriter(file, false);
        writer.write("@E_TIME@=" + startTime + "\r\n");
        writer.write("@username@=" + username + "\r\n");
        writer.write("@password@=" + passwd + "\r\n");
        writer.close();
    }

    public void testDeleteRows()
        throws Exception
    {
        binding = (SoapBindingStub) new SforceServiceLocator().getSoap();

        // timeout after 1 min
        binding.setTimeout(60000);
        LoginResult loginResult = binding.login(username, passwd);
        binding._setProperty(
            SoapBindingStub.ENDPOINT_ADDRESS_PROPERTY,
            loginResult.getServerUrl());
        // Create a new session header object and set the session id to that
        // returned by the login
        SessionHeader sh = new SessionHeader();
        sh.setSessionId(loginResult.getSessionId());
        binding.setHeader(
            new SforceServiceLocator().getServiceName().getNamespaceURI(),
            "SessionHeader",
            sh);

        // Get the id file
        String platformHome = System.getenv("SFDC_TEST_HOME");
        char sep = File.separatorChar;
        String filePath = "";
        File file = new File(platformHome, filePath+"ids");
        assert(file.exists());
        FileReader reader = new FileReader(file);

        // Gets the ids for the rows to delete
        StringBuilder idBuffer = new StringBuilder(20);
        char inChar;
        for (int i = 0; i < 2; i++) {
            while ((inChar = (char)reader.read()) != -1) {
                if (inChar == '\n') {
                    break;
                } else {
                    idBuffer.append(inChar);
                }
            }
            ids[i] = idBuffer.toString();
            idBuffer.delete(0,idBuffer.length());
        }

        DeleteResult[] deleteResults = binding.delete(ids);

        boolean hasDeleteErrors = false;
        // Process the results
        for (int i=0;i<deleteResults.length;i++) {
            DeleteResult deleteResult = deleteResults[i];
            // Determine whether delete succeeded or had errors
            if (deleteResult.isSuccess()) {
                // Get the id of the deleted record
                deleteResult.getId();
            }
            else {
                // Handle the errors
                // don't fail test if we can't cleanup accounts
                System.out.println("Error deleting Account");
                hasDeleteErrors = true;
//                 throw new junit.framework.AssertionFailedError(
//                     "Error deleting objects");
            }
        }

        // delete ids file if all records correctly deleted
        reader.close();
        if (!hasDeleteErrors) {
            file.delete();
        }
    }


    // modified example from sfdc API docs
    private void createAccountSample()
        throws Exception
    {
        // Create two account objects
        Account account1 = new Account();
        Account account2 = new Account();

        // Set some fields on the account object
        // Name field (required) not being set on account1,
        // so this record should fail during create.
        account1.setName("New Account");
        account1.setBillingCity("Wichita");
        account1.setBillingCountry("US");
        account1.setBillingState("KA");
        account1.setBillingStreet("4322 Haystack Boulevard");
        account1.setBillingPostalCode("87901");

        // Set some fields on the account2 object
        account2.setName("Golden Straw");
        account2.setBillingCity("Oakland");
        account2.setBillingCountry("US");
        account2.setBillingState("CA");
        account2.setBillingStreet("666 Raiders Boulevard");
        account2.setBillingPostalCode("97502");

        // Create an array of SObjects to hold the accounts
        SObject[] sObjects = new SObject[2];
        // Add the accounts to the SObject array
        sObjects[0] = account1;
        sObjects[1] = account2;

        // Invoke the create call
        SaveResult[] saveResults = binding.create(sObjects);

        // Handle the results
        for (int i=0; i<saveResults.length; i++) {
            // Determine whether create succeeded or had errors
            if (saveResults[i].isSuccess()) {
                // No errors, so we will retrieve the id created for this index
                ids[i] = saveResults[i].getId();
            }
            else {
                // Handle the errors
                throw new junit.framework.AssertionFailedError(
                    "Error creating objects");
            }
        }
    }

    // modified example from sfdc API docs
    private void deleteSample()
        throws Exception
    {
        DeleteResult[] deleteResults = binding.delete(ids);
        // Process the results
        for (int i=0;i<deleteResults.length;i++) {
            DeleteResult deleteResult = deleteResults[i];
            // Determine whether delete succeeded or had errors
            if (deleteResult.isSuccess()) {
                // Get the id of the deleted record
                deleteResult.getId();
            }
            else {
                // Handle the errors
                throw new junit.framework.AssertionFailedError(
                    "Error deleting objects");
            }
        }
    }

}

// End CreateAndDeleteTest.java
