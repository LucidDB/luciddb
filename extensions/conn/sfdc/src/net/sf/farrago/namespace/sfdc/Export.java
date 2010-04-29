/*
// $Id$
// SFDC Connector is a SQL/MED connector for Salesforce.com for Farrago
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
package net.sf.farrago.namespace.sfdc;

import com.sforce.soap.partner.*;
import com.sforce.soap.partner.fault.*;
import com.sforce.soap.partner.sobject.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.rmi.RemoteException;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.rpc.ServiceException;

import net.sf.farrago.namespace.sfdc.resource.*;

import org.apache.axis.message.MessageElement;


/**
 * Utility to export sfdc objects to csv
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class Export
{
    //~ Static fields/initializers ---------------------------------------------

    private static boolean compress = false;

    // Used for testing to limit rows returned. If set to -1, means no limit.
    private static int maxRows = -1;
    private static int batchsize = 1000;
    private static boolean withbcp = false;
    private static boolean quoteall = false;

    private static final String QUOTE = "\"";
    private static final String TAB = "\t";
    private static final String NEWLINE = "\r\n";

    //~ Instance fields --------------------------------------------------------

    private SoapBindingStub binding;

    //~ Methods ----------------------------------------------------------------

    public static void main(String [] args)
    {
        Export export = new Export();

        String user = "";
        String pass = "";
        String ob = "";
        String start = "";
        String end = "";
        boolean cdc = false;

        for (int i = 0; i < args.length; i++) {
            int valIndex = args[i].indexOf('=');
            if (valIndex < 0) {
                if (args[i].equals("--cdc")) {
                    cdc = true;
                    continue;
                }
                usage();
            }
            String option = args[i].substring(0, valIndex);
            if (option.equals("-user")) {
                user = args[i].substring(valIndex + 1);
                continue;
            }
            if (option.equals("-pass")) {
                pass = args[i].substring(valIndex + 1);
                continue;
            }
            if (option.equals("-objects")) {
                ob = args[i].substring(valIndex + 1);
                continue;
            }
            if (option.equals("-withbcp")) {
                if ((args[i].substring(valIndex + 1).toLowerCase()).equals("y")
                    || (args[i].substring(valIndex + 1).toLowerCase()).equals(
                        "yes"))
                {
                    withbcp = true;
                }
                continue;
            }
            if (option.equals("-quoteall")) {
                if ((args[i].substring(valIndex + 1).toLowerCase()).equals("y")
                    || (args[i].substring(valIndex + 1).toLowerCase()).equals(
                        "yes"))
                {
                    quoteall = true;
                }
                continue;
            }
            if (option.equals("--cdc")) {
                if ((args[i].substring(valIndex + 1).toLowerCase()).equals(
                        "true"))
                {
                    cdc = true;
                }
                continue;
            }
            if (option.equals("-start")) {
                start = args[i].substring(valIndex + 1);
                continue;
            }
            if (option.equals("-end")) {
                end = args[i].substring(valIndex + 1);
                continue;
            }
            if (option.equals("-compress")) {
                if ((args[i].substring(valIndex + 1).toLowerCase()).equals(
                        "true"))
                {
                    compress = true;
                }
                continue;
            }
            if (option.equals("-maxrows")) {
                Integer r = new Integer(args[i].substring(valIndex + 1));
                maxRows = r.intValue();
                System.out.println(
                    "number of rows per object limited to " + maxRows);
                continue;
            }
            if (option.equals("-batchsize")) {
                Integer r = new Integer(args[i].substring(valIndex + 1));
                batchsize = r.intValue();
                System.out.println("batchsize set to " + batchsize);
                continue;
            }

            usage();
        }
        export.login(user, pass);

        // if no objects specified, default to get all objects
        if (ob.equals("")) {
            ob = export.getAllObjects();
        }
        if (cdc) {
            export.getChanges(ob, start, end);
        } else {
            export.toCsv(ob);
        }
    }

    private void login(String user, String pass)
    {
        if (compress) {
            System.out.println("Compression Enabled");
            try {
                binding = (SoapBindingStub) new ServiceLocatorGzip().getSoap();
            } catch (ServiceException se) {
                throw SfdcResourceObject.get().SfdcBinding_ServiceException.ex(
                    se.getMessage());
            }
        } else {
            try {
                System.out.println("Compression Disabled");
                binding =
                    (SoapBindingStub) new SforceServiceLocator().getSoap();
            } catch (ServiceException se) {
                throw SfdcResourceObject.get().SfdcBinding_ServiceException.ex(
                    se.getMessage());
            }
        }

        // timeout after 1 min
        binding.setTimeout(60000);

        LoginResult loginResult = null;
        try {
            loginResult = binding.login(user, pass);
        } catch (LoginFault lf) {
            throw SfdcResourceObject.get().SfdcLoginFault.ex(
                lf.getExceptionMessage());
        } catch (UnexpectedErrorFault uef) {
            throw SfdcResourceObject.get().SfdcLoginFault.ex(
                uef.getExceptionMessage());
        } catch (RemoteException re) {
            throw SfdcResourceObject.get().SfdcLoginFault.ex(re.getMessage());
        }

        // set the session header for subsequent call authentication
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
    }

    private void toCsv(String obs)
    {
        // create 1 csv per data object specified
        String [] objNames = obs.split("\\s*,\\s*");

        String literalSep = "\"\\t\"";
        String lastLitSep = "\"\\r\\n\"";
        File csvFile = null;
        File bcpFile = null;
        FileWriter csvOut = null;
        FileWriter bcpOut = null;
        java.util.Date now = new java.util.Date();
        String parentDir = "_conf_" + now.toString();
        try {
            parentDir =
                binding.getUserInfo().getOrganizationName().concat(
                    parentDir);
        } catch (RemoteException re) {
            // do nothing; conf dir will not have org name set
        }

        // get rid of spaces and colons in directory name
        parentDir = parentDir.replaceAll("\\s*", "");
        parentDir = parentDir.replaceAll(":", "");
        File pdir = new File(parentDir);
        pdir.mkdirs();

        boolean [] quotes = null;

        for (int i = 0; i < objNames.length; i++) {
            String csvName = objNames[i] + ".txt";
            String bcpName = objNames[i] + ".bcp";
            try {
                csvFile = new File(parentDir + File.separator + csvName);
                csvOut = new FileWriter(csvFile, false);
                if (withbcp) {
                    bcpFile = new File(parentDir + File.separator + bcpName);
                    bcpOut = new FileWriter(bcpFile, false);
                    bcpOut.write("6.0" + NEWLINE); // version using BroadBase
                }

                DescribeSObjectResult describeSObjectResult =
                    binding.describeSObject(objNames[i]);
                com.sforce.soap.partner.Field [] fields =
                    describeSObjectResult.getFields();
                String fieldlist = "";
                if (withbcp) {
                    bcpOut.write(fields.length + NEWLINE);
                }
                quotes = new boolean[fields.length];

                // add all fields to .txt
                for (int j = 0; j < fields.length; j++) {
                    String fieldName = fields[j].getName();
                    String fieldType = fields[j].getType().toString();
                    boolean isCustom = fields[j].isCustom();

                    if (isQuoteableField(fieldType)) {
                        quotes[j] = true;
                    } else {
                        quotes[j] = false;
                    }

                    // BB: limit fieldname in BCP file to 30 char
                    String bcpFieldName;
                    if (fieldName.length() <= 30) {
                        bcpFieldName = fieldName;
                    } else {
                        if (isCustom) {
                            bcpFieldName = fieldName.substring(0, 27);
                            bcpFieldName = bcpFieldName.concat("__c");
                        } else {
                            bcpFieldName = fieldName.substring(0, 30);
                        }
                    }

                    // the column names, tab separated, quoted.
                    if (j == 0) {
                        csvOut.write(QUOTE);
                    }
                    csvOut.write(bcpFieldName);
                    fieldlist = fieldlist.concat(fieldName);

                    if (j != (fields.length - 1)) {
                        csvOut.write(QUOTE + TAB + QUOTE);
                        fieldlist = fieldlist.concat(", ");
                        if (withbcp) {
                            bcpOut.write(
                                (j + 1) + TAB
                                + fieldType.toLowerCase() + TAB + literalSep
                                + TAB + (j + 1) + TAB + bcpFieldName + NEWLINE);
                        }
                    } else {
                        csvOut.write(QUOTE + NEWLINE);
                        if (withbcp) {
                            bcpOut.write(
                                (j + 1) + TAB
                                + fieldType.toLowerCase() + TAB + lastLitSep
                                + TAB + (j + 1) + TAB + bcpFieldName + NEWLINE);
                        }
                    }
                }

                // query, selecting all fields
                QueryOptions qo = new QueryOptions();
                qo.setBatchSize(new Integer(batchsize));
                binding.setHeader(
                    new SforceServiceLocator().getServiceName()
                                              .getNamespaceURI(),
                    "QueryOptions",
                    qo);
                QueryResult qr =
                    binding.query(
                        "select " + fieldlist + " from "
                        + objNames[i]);
                SObject [] records = qr.getRecords();
                if (records != null) {
                    boolean bContinue = true;
                    int rows = 0;
                    while (bContinue) {
                        // for each record returned in query,
                        // get value of each field
                        for (int j = 0; j < records.length; j++) {
                            MessageElement [] elements = records[j].get_any();
                            if (elements != null) {
                                for (int k = 0; k < elements.length; k++) {
                                    MessageElement elt = elements[k];

                                    if (elt.getValue() != null) {
                                        if (quotes[k] == true) {
                                            csvOut.write(QUOTE);
                                        }

                                        // quote the quotes
                                        csvOut.write(quote(elt.getValue()));

                                        if (quotes[k] == true) {
                                            csvOut.write(QUOTE);
                                        }
                                    }
                                    if (k != (elements.length - 1)) {
                                        csvOut.write(TAB);
                                    }
                                }
                            }
                            csvOut.write(NEWLINE);
                            rows++;
                        }
                        if (qr.isDone()
                            || ((rows >= maxRows) && (maxRows != -1)))
                        {
                            System.out.println("Completed Rows = " + rows);
                            bContinue = false;
                        } else {
                            qr = binding.queryMore(qr.getQueryLocator());
                            records = qr.getRecords();
                        }
                    }
                } else {
                    // if no records for object, delete the .txt
                    csvOut.close();
                    csvFile.delete();
                }
            } catch (RemoteException re) {
                System.out.println(
                    SfdcResourceObject.get().ObjectQueryExceptionMsg.str(
                        objNames[i],
                        re.getMessage()));
                try {
                    csvOut.close();
                } catch (IOException ie) {
                    System.out.println(
                        SfdcResourceObject.get().IOExceptionMsg.str(
                            ie.getMessage()));
                }
                csvFile.delete();
                if (withbcp) {
                    try {
                        bcpOut.close();
                    } catch (IOException ie) {
                        System.out.println(
                            SfdcResourceObject.get().IOExceptionMsg.str(
                                ie.getMessage()));
                    }
                    bcpFile.delete();
                }
                continue;
            } catch (IOException ie) {
                System.out.println(
                    SfdcResourceObject.get().IOExceptionMsg.str(
                        ie.getMessage()));
                continue;
            } finally {
                try {
                    csvOut.close();
                    if (withbcp) {
                        bcpOut.close();
                    }
                } catch (IOException ie) {
                    System.out.println(
                        SfdcResourceObject.get().IOExceptionMsg.str(
                            ie.getMessage()));
                }
            }
        }
    }

    private String quote(String value)
    {
        return value.replaceAll("\"", "\"\"");
    }

    private boolean isQuoteableField(String type)
    {
        if ((type.toLowerCase().equals("string"))
            || (type.toLowerCase().equals("base64"))
            || (type.toLowerCase().equals("base64binary"))
            || (type.toLowerCase().equals("id"))
            || (type.toLowerCase().equals("reference"))
            || (type.toLowerCase().equals("textarea"))
            || (type.toLowerCase().equals("phone"))
            || (type.toLowerCase().equals("url"))
            || (type.toLowerCase().equals("email"))
            || (type.toLowerCase().equals("picklist"))
            || (type.toLowerCase().equals("multipicklist"))
            || (type.toLowerCase().equals("combobox"))
            || (type.toLowerCase().equals("anytype"))
            || (type.toLowerCase().equals("calculated")))
        {
            return true;
        }

        // FIXME: timestamp => varchar again.
        if (type.toLowerCase().equals("datetime")) {
            return true;
        }

        return quoteall;
    }

    private void getChanges(String obs, String startTime, String endTime)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        Date sd = sdf.parse(startTime, new ParsePosition(0));

        GregorianCalendar start = new GregorianCalendar();
        start.setTime(sd);

        GregorianCalendar end = new GregorianCalendar();
        sd = sdf.parse(endTime, new ParsePosition(0));
        end.setTime(sd);

        String [] objNames = obs.split("\\s*,\\s*");

        // note: does not check for structural changes
        getUpdated(objNames, start, end);
        getDeleted(objNames, start, end);
    }

    private void getUpdated(
        String [] objNames,
        GregorianCalendar start,
        GregorianCalendar end)
    {
        File updateFile = null;
        FileWriter updateW = null;
        java.util.Date now = new java.util.Date();
        String parentDir = "_conf_cdc";
        try {
            parentDir =
                binding.getUserInfo().getOrganizationName().concat(
                    parentDir);
        } catch (RemoteException re) {
            // do nothing; conf dir will not have org name set
        }
        parentDir = parentDir.replaceAll("\\s*", "");
        parentDir = parentDir.replaceAll(":", "");
        File pdir = new File(parentDir);
        pdir.mkdirs();

        boolean [] quotes = null;

        for (int i = 0; i < objNames.length; i++) {
            try {
                DescribeSObjectResult describeSObjectResult =
                    binding.describeSObject(objNames[i]);

                // check if data replication is allowed on object
                if (!describeSObjectResult.isReplicateable()) {
                    System.out.println(objNames[i] + " is not replicateable");
                    continue;
                }

                String updateName = objNames[i] + "_upd.txt";
                updateFile = new File(parentDir + File.separator + updateName);
                updateW = new FileWriter(updateFile, false);

                // fieldlist
                com.sforce.soap.partner.Field [] fields =
                    describeSObjectResult.getFields();
                String fieldlist = "";
                quotes = new boolean[fields.length];
                for (int j = 0; j < fields.length; j++) {
                    fieldlist = fieldlist.concat(fields[j].getName());
                    if (j != (fields.length - 1)) {
                        fieldlist = fieldlist.concat(", ");
                    }

                    String fieldType = fields[j].getType().toString();
                    if (isQuoteableField(fieldType)) {
                        quotes[j] = true;
                    } else {
                        quotes[j] = false;
                    }
                }

                GetUpdatedResult ur =
                    binding.getUpdated(
                        objNames[i],
                        start,
                        end);
                if ((ur.getIds() != null) && (ur.getIds().length > 0)) {
                    // retrieve the updated data:
                    SObject [] result =
                        binding.retrieve(
                            fieldlist,
                            objNames[i],
                            ur.getIds());
                    if (result != null) {
                        for (int j = 0; j < result.length; j++) {
                            MessageElement [] elements = result[j].get_any();
                            if (elements != null) {
                                for (int k = 0; k < elements.length; k++) {
                                    MessageElement elt = elements[k];
                                    if (quotes[k] == true) {
                                        updateW.write(QUOTE);
                                    }

                                    if (elt.getValue() != null) {
                                        updateW.write(quote(elt.getValue()));
                                    }

                                    if (k != (elements.length - 1)) {
                                        if (quotes[k] == true) {
                                            updateW.write(QUOTE);
                                            updateW.write(TAB);
                                        } else {
                                            updateW.write(TAB);
                                        }
                                    }
                                }

                                // last column
                                if (quotes[elements.length - 1] == true) {
                                    updateW.write(QUOTE);
                                }
                            }
                            updateW.write(NEWLINE);
                        }
                    } else {
                        updateW.close();
                    }
                } else {
                    updateW.close();
                    updateFile.delete();
                }
            } catch (RemoteException ex) {
                System.out.println(
                    SfdcResourceObject.get().ObjectQueryExceptionMsg.str(
                        objNames[i],
                        ex.getMessage()));
                exit();
            } catch (IOException ie) {
                System.out.println(
                    SfdcResourceObject.get().IOExceptionMsg.str(
                        ie.getMessage()));
                exit();
            } finally {
                try {
                    updateW.close();
                } catch (IOException ie) {
                    System.out.println(
                        SfdcResourceObject.get().IOExceptionMsg.str(
                            ie.getMessage()));
                }
            }
        }
    }

    private void getDeleted(
        String [] objNames,
        GregorianCalendar start,
        GregorianCalendar end)
    {
        File deleteFile = null;
        FileWriter deleteW = null;
        java.util.Date now = new java.util.Date();
        String parentDir = "_conf_cdc";
        try {
            parentDir =
                binding.getUserInfo().getOrganizationName().concat(
                    parentDir);
        } catch (RemoteException re) {
            // do nothing; conf dir will not have org name set
        }
        parentDir = parentDir.replaceAll("\\s*", "");
        parentDir = parentDir.replaceAll(":", "");
        File pdir = new File(parentDir);
        pdir.mkdirs();

        for (int i = 0; i < objNames.length; i++) {
            try {
                DescribeSObjectResult describeSObjectResult =
                    binding.describeSObject(objNames[i]);

                // check if data replication is allowed on object
                if (!describeSObjectResult.isReplicateable()) {
                    continue;
                }

                String deleteName = objNames[i] + "_del.txt";
                deleteFile = new File(parentDir + File.separator + deleteName);
                deleteW = new FileWriter(deleteFile, false);

                GetDeletedResult gdr =
                    binding.getDeleted(
                        objNames[i],
                        start,
                        end);
                if ((gdr.getDeletedRecords() != null)
                    && (gdr.getDeletedRecords().length > 0))
                {
                    for (int j = 0; j < gdr.getDeletedRecords().length; j++) {
                        deleteW.write(gdr.getDeletedRecords(j).getId());
                        deleteW.write(NEWLINE);
                    }
                    deleteW.close();
                } else {
                    deleteW.close();
                    deleteFile.delete();
                }
            } catch (RemoteException ex) {
                System.out.println(
                    SfdcResourceObject.get().ObjectQueryExceptionMsg.str(
                        objNames[i],
                        ex.getMessage()));
            } catch (IOException ie) {
                System.out.println(
                    SfdcResourceObject.get().IOExceptionMsg.str(
                        ie.getMessage()));
            } finally {
                try {
                    deleteW.close();
                } catch (IOException ie) {
                    System.out.println(
                        SfdcResourceObject.get().IOExceptionMsg.str(
                            ie.getMessage()));
                }
            }
        }
    }

    private String getAllObjects()
    {
        try {
            DescribeGlobalResult describeGlobalResult =
                binding.describeGlobal();
            if (!(describeGlobalResult == null)) {
                // Get the array of object names
                DescribeGlobalSObjectResult [] types =
                    describeGlobalResult.getSobjects();
                if (!(types == null)) {
                    return arrayToString(types, ",");
                } else {
                }
            }
        } catch (RemoteException re) {
            System.out.println(
                SfdcResourceObject.get().AllObjectQueryExceptionMsg.str(
                    re.getMessage()));
        }
        return "Account";
    }

    private String arrayToString(
        DescribeGlobalSObjectResult [] a,
        String separator)
    {
        StringBuffer result = new StringBuffer();
        if (a.length > 0) {
            result.append(a[0].getName());
            for (int i = 1; i < a.length; i++) {
                result.append(separator);
                result.append(a[i].getName());
            }
        }
        return result.toString();
    }

    private static void usage()
    {
        System.out.println(
            "Export -user=\"username\" " + "-pass=\"passwd\" "
            + "-objects=\"object,object2\" "
            + "[--cdc -start=[timestamp] -end=[timestamp]] "
            + "with [timestamp] in format: yyyy-MM-dd'T'HH:mm\n");
        exit();
    }

    private static void exit()
    {
        System.exit(-1);
    }
}
// End Export.java
