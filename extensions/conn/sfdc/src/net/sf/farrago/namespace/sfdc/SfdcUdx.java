/*
// $Id$
// SFDC Connector is a SQL/MED connector for Salesforce.com for Farrago
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 DynamoBI Corporation
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
import com.sforce.soap.partner.sobject.*;

import java.rmi.RemoteException;

import java.sql.*;

import java.text.*;

import java.util.*;

import net.sf.farrago.namespace.sfdc.resource.*;
import net.sf.farrago.resource.FarragoResource;
import net.sf.farrago.runtime.FarragoUdrRuntime;

import org.apache.axis.*;
import org.apache.axis.message.MessageElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * UDX to query sfdc.
 *
 * @author Sunny Choi
 * @version $Id$
 */
public abstract class SfdcUdx
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int RETRY_CNT = 3;
    protected static final Log log = LogFactory.getLog(SfdcUdx.class);

    //~ Methods ----------------------------------------------------------------

    public static void query(
        String query,
        String types,
        PreparedStatement resultInserter)
        throws SQLException
    {
        SoapBindingStub binding =
            (SoapBindingStub) FarragoUdrRuntime.getDataServerRuntimeSupport(
                null);

        try {
            QueryOptions qo = new QueryOptions();
            int batchsize = 500;
            qo.setBatchSize(new Integer(batchsize));
            binding.setHeader(
                new SforceServiceLocator().getServiceName().getNamespaceURI(),
                "QueryOptions",
                qo);

            String objName = query;
            int fromIdx = query.lastIndexOf(" from");
            if (fromIdx > 0) {
                objName = query.substring(fromIdx + 6);
            }

            // strip off quotes for boolean values
            query = stripQuotes(query, objName);
            log.info("SFDC Query: " + query);

            QueryResult qr = binding.query(query);
            if (qr.isDone()) {
                if (qr.getRecords() != null) {
                    log.info(
                        SfdcResourceObject.get().RetrievedAllRecordsMsg.str(
                            Integer.toString(qr.getRecords().length),
                            objName));
                }
            } else {
                if (qr.getRecords() != null) {
                    log.info(
                        SfdcResourceObject.get().RetrievingRecordsMsg.str(
                            Integer.toString(qr.getRecords().length),
                            objName));
                }
            }
            SObject [] records = qr.getRecords();
            String [] metadataType = types.split(",");

            // query is of following format:
            // "select col1,col2,... from"
            String cols = query.substring(7);
            fromIdx = cols.lastIndexOf(" from");
            cols = cols.substring(0, fromIdx);
            cols = cols.trim();
            String [] columnValues = new String[metadataType.length];

            if (records != null) {
                boolean bContinue = true;
                while (bContinue) {
                    // for each record returned in query,
                    // get value of each field
                    for (int i = 0; i < records.length; i++) {
                        MessageElement [] elements = records[i].get_any();
                        if (elements != null) {
                            for (int j = 0; j < elements.length; j++) {
                                MessageElement elt = elements[j];
                                String eltVal = elt.getValue();
                                columnValues[j] =
                                    (eltVal != null) ? eltVal : "null";

                                if (metadataType[j].indexOf("TIMESTAMP")
                                    != -1)
                                {
                                    // TIMESTAMP
                                    if (eltVal != null) {
                                        String tstampstr =
                                            eltVal.replace(
                                                "T",
                                                " ");
                                        tstampstr =
                                            tstampstr.substring(
                                                0,
                                                tstampstr.indexOf("."));
                                        java.sql.Timestamp tstamp =
                                            java.sql.Timestamp.valueOf(
                                                tstampstr);
                                        resultInserter.setTimestamp(
                                            j + 1,
                                            tstamp);
                                    } else {
                                        resultInserter.setNull(
                                            j + 1,
                                            java.sql.Types.TIMESTAMP);
                                    }
                                } else if (
                                    metadataType[j].indexOf("TIME")
                                    != -1)
                                {
                                    // TIME
                                    if (eltVal != null) {
                                        String timestr =
                                            eltVal.substring(
                                                0,
                                                eltVal.indexOf("."));
                                        java.sql.Time time =
                                            java.sql.Time.valueOf(timestr);
                                        resultInserter.setTime(j + 1, time);
                                    } else {
                                        resultInserter.setNull(
                                            j + 1,
                                            java.sql.Types.TIME);
                                    }
                                } else if (
                                    metadataType[j].indexOf("DATE")
                                    != -1)
                                {
                                    // DATE
                                    if (eltVal != null) {
                                        java.sql.Date dt =
                                            java.sql.Date.valueOf(eltVal);
                                        resultInserter.setDate(j + 1, dt);
                                    } else {
                                        resultInserter.setNull(
                                            j + 1,
                                            java.sql.Types.DATE);
                                    }
                                } else if (
                                    metadataType[j].indexOf("INTEGER")
                                    != -1)
                                {
                                    // INTEGER
                                    if (eltVal != null) {
                                        int iValue = 0;
                                        iValue = Integer.parseInt(eltVal);
                                        resultInserter.setInt(j + 1, iValue);
                                    } else {
                                        resultInserter.setNull(
                                            j + 1,
                                            java.sql.Types.INTEGER);
                                    }
                                } else if (
                                    metadataType[j].indexOf("DOUBLE")
                                    != -1)
                                {
                                    // DOUBLE
                                    if (eltVal != null) {
                                        resultInserter.setDouble(
                                            j + 1,
                                            Double.parseDouble(eltVal));
                                    } else {
                                        resultInserter.setNull(
                                            j + 1,
                                            java.sql.Types.DOUBLE);
                                    }
                                } else if (eltVal != null) {
                                    // VARCHAR - default
                                    int rightParen =
                                        metadataType[j].indexOf(")");
                                    int prec =
                                        Integer.parseInt(
                                            metadataType[j].substring(
                                                8,
                                                rightParen));
                                    if (eltVal.length() > prec) {
                                        eltVal = eltVal.substring(0, prec);
                                        columnValues[j] = eltVal;
                                    }
                                    resultInserter.setString(j + 1, eltVal);
                                } else {
                                    resultInserter.setNull(
                                        j + 1,
                                        java.sql.Types.VARCHAR);
                                }
                            }
                            resultInserter.executeUpdate();
                        }
                    }
                    if (qr.isDone()) {
                        bContinue = false;
                    } else {
                        boolean relogin = true;
                        int retryCnt = 0;
                        while (relogin) {
                            try {
                                qr = binding.queryMore(qr.getQueryLocator());
                                relogin = false;
                            } catch (AxisFault a) {
                                if (a.getFaultString().contains(
                                        "Invalid Session ID")
                                    && (retryCnt < RETRY_CNT))
                                {
                                    relogin = true;
                                    retryCnt++;
                                    binding =
                                        (SoapBindingStub) FarragoUdrRuntime
                                        .getDataServerRuntimeSupport(binding);
                                } else {
                                    throw a;
                                }
                            }
                        }
                        records = qr.getRecords();
                        if (qr.isDone()) {
                            if (qr.getRecords() != null) {
                                log.info(
                                    SfdcResourceObject.get()
                                    .RetrievedAllRecordsMsg.str(
                                        Integer.toString(
                                            qr.getRecords().length),
                                        objName));
                            }
                        } else {
                            if (qr.getRecords() != null) {
                                log.info(
                                    SfdcResourceObject.get()
                                    .RetrievingRecordsMsg.str(
                                        Integer.toString(
                                            qr.getRecords().length),
                                        objName));
                            }
                        }
                    }
                }
            }
        } catch (AxisFault ae) {
            SQLException retryExcn =
                new SQLException(
                    ae.getFaultString(),
                    null,
                    460150);
            Exception chainedEx =
                FarragoResource.instance().RetryableFailure.ex(retryExcn);
            throw SfdcResourceObject.get().BindingCallException.ex(
                ae.getFaultString(),
                chainedEx);
        } catch (RemoteException re) {
            SQLException retryExcn =
                new SQLException(
                    re.getMessage(),
                    null,
                    460150);
            Exception chainedEx =
                FarragoResource.instance().RetryableFailure.ex(retryExcn);
            throw SfdcResourceObject.get().BindingCallException.ex(
                re.getMessage(),
                chainedEx);
        }
    }

    public static void getDeleted(
        String objectName,
        String start,
        String end,
        PreparedStatement resultInserter)
        throws SQLException
    {
        SoapBindingStub binding =
            (SoapBindingStub) FarragoUdrRuntime.getDataServerRuntimeSupport(
                null);

        if (((start == null) || start.equals(""))
            || ((end == null) || end.equals("")))
        {
            throw SfdcResourceObject.get().InvalidRangeException.ex();
        }

        Calendar startTime;
        Calendar endTime;
        Calendar thirtyDaysAgo;

        try {
            SimpleDateFormat sdf =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            java.util.Date sd = sdf.parse(start, new ParsePosition(0));
            startTime = Calendar.getInstance();
            startTime.setTime(sd);

            java.util.Date now = new java.util.Date();

            // 30 days == 30*24*60*60*1000 ms
            Long thirty = new Long("2592000000");
            java.util.Date thirtyDate =
                new java.util.Date(
                    now.getTime()
                    - thirty.longValue());
            thirtyDaysAgo = Calendar.getInstance();
            thirtyDaysAgo.setTime(thirtyDate);

            java.util.Date ed = sdf.parse(end, new ParsePosition(0));
            endTime = Calendar.getInstance();
            endTime.setTime(ed);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw SfdcResourceObject.get().InvalidTimeException.ex(
                ex.getMessage());
        }
        if (thirtyDaysAgo.compareTo(startTime) > 0) {
            throw SfdcResourceObject.get().InvalidStartTimeException.ex(
                startTime.getTime().toString());
        }

        if (startTime.compareTo(endTime) > 0) {
            throw SfdcResourceObject.get().InvalidEndTimeException.ex(
                endTime.getTime().toString(),
                startTime.getTime().toString());
        }

        SfdcDataServer server =
            (SfdcDataServer) FarragoUdrRuntime.getDataServerRuntimeSupport(
                new Object());
        try {
            DescribeSObjectResult describeSObjectResult =
                (DescribeSObjectResult) server.getEntityDescribe(objectName);

            // check the name
            if ((describeSObjectResult != null)
                && describeSObjectResult.getName().equals(objectName))
            {
                // check if data replication is allowed on object
                if (!describeSObjectResult.isReplicateable()) {
                    throw SfdcResourceObject.get().ReplicationException.ex(
                        objectName);
                }
            } else {
                throw SfdcResourceObject.get().InvalidObjectException.ex(
                    objectName);
            }
            GetDeletedResult gdr =
                binding.getDeleted(
                    objectName,
                    startTime,
                    endTime);
            if ((gdr.getDeletedRecords() != null)
                && (gdr.getDeletedRecords().length > 0))
            {
                for (int i = 0; i < gdr.getDeletedRecords().length; i++) {
                    SimpleDateFormat sdf =
                        new SimpleDateFormat(
                            "yyyy-MM-dd HH:mm:ss");
                    StringBuffer sbuf = new StringBuffer();
                    String idString = gdr.getDeletedRecords(i).getId();
                    int prec = 25 + server.getVarcharPrecision();
                    if (idString.length() > prec) {
                        idString = idString.substring(0, prec);
                    }
                    resultInserter.setString(1, idString);
                    String timeStr =
                        sdf.format(
                            gdr.getDeletedRecords(i).getDeletedDate().getTime(),
                            sbuf,
                            new FieldPosition(0)).toString();
                    resultInserter.setTimestamp(
                        2,
                        java.sql.Timestamp.valueOf(timeStr));
                    resultInserter.executeUpdate();
                }
            }
        } catch (AxisFault ae) {
            SQLException retryExcn =
                new SQLException(
                    ae.getFaultString(),
                    null,
                    460150);
            Exception chainedEx =
                FarragoResource.instance().RetryableFailure.ex(retryExcn);
            throw SfdcResourceObject.get().BindingCallException.ex(
                ae.getFaultString(),
                chainedEx);
        } catch (RemoteException re) {
            SQLException retryExcn =
                new SQLException(
                    re.getMessage(),
                    null,
                    460150);
            Exception chainedEx =
                FarragoResource.instance().RetryableFailure.ex(retryExcn);
            throw SfdcResourceObject.get().BindingCallException.ex(
                re.getMessage(),
                chainedEx);
        }
    }

    public static void getLov(
        String objectName,
        PreparedStatement resultInserter)
        throws SQLException
    {
        try {
            SfdcDataServer server =
                (SfdcDataServer) FarragoUdrRuntime.getDataServerRuntimeSupport(
                    new Object());
            DescribeSObjectResult describeSObjectResult =
                (DescribeSObjectResult) server.getEntityDescribe(objectName);

            // check the name
            if ((describeSObjectResult != null)
                && describeSObjectResult.getName().equals(objectName))
            {
                com.sforce.soap.partner.Field [] fields =
                    describeSObjectResult.getFields();

                for (int i = 0; i < fields.length; i++) {
                    PicklistEntry [] picklistValues =
                        fields[i].getPicklistValues();
                    if (picklistValues != null) {
                        for (int j = 0; j < picklistValues.length; j++) {
                            if (picklistValues[j].getLabel() != null) {
                                String fieldString = fields[i].getName();
                                int prec = 25 + server.getVarcharPrecision();
                                if (fieldString.length() > prec) {
                                    fieldString =
                                        fieldString.substring(0, prec);
                                }
                                resultInserter.setString(1, fieldString);
                                String lovValue = picklistValues[j].getValue();
                                prec = 256 + server.getVarcharPrecision();
                                if (lovValue.length() > prec) {
                                    lovValue = lovValue.substring(0, prec);
                                }
                                resultInserter.setString(2, lovValue);
                                resultInserter.executeUpdate();
                            }
                        }
                    }
                }
            } else {
                throw SfdcResourceObject.get().InvalidObjectException.ex(
                    objectName);
            }
        } catch (AxisFault ae) {
            SQLException retryExcn =
                new SQLException(
                    ae.getFaultString(),
                    null,
                    460150);
            Exception chainedEx =
                FarragoResource.instance().RetryableFailure.ex(retryExcn);
            throw SfdcResourceObject.get().BindingCallException.ex(
                ae.getFaultString(),
                chainedEx);
        } catch (RemoteException re) {
            SQLException retryExcn =
                new SQLException(
                    re.getMessage(),
                    null,
                    460150);
            Exception chainedEx =
                FarragoResource.instance().RetryableFailure.ex(retryExcn);
            throw SfdcResourceObject.get().BindingCallException.ex(
                re.getMessage(),
                chainedEx);
        }
    }

    private static String stripQuotes(String query, String object)
        throws RemoteException, SQLException
    {
        String queryString = query;
        String filter = null;
        int fromIdx = query.lastIndexOf(" where");
        if (fromIdx > 0) {
            filter = query.substring(fromIdx + 7);
        }
        if (filter != null) {
            String [] filterArray = filter.split(" ");
            for (int i = 0; i < filterArray.length; i++) {
                if (filterArray[i].equals("=")
                    || filterArray[i].equals("<>")
                    || filterArray[i].equals("!="))
                {
                    SfdcDataServer server =
                        (SfdcDataServer) FarragoUdrRuntime
                        .getDataServerRuntimeSupport(new Object());
                    DescribeSObjectResult describeObj =
                        server.getEntityDescribe(object.split(" ")[0]);
                    Field [] fields = describeObj.getFields();
                    int lastRightParen = filterArray[i - 1].lastIndexOf('(');
                    String condField =
                        filterArray[i - 1].substring(lastRightParen + 1);
                    for (int j = 0; j < fields.length; j++) {
                        if (fields[j].getName().equalsIgnoreCase(condField)) {
                            if (fields[j].getType().getValue().equalsIgnoreCase(
                                    "boolean"))
                            {
                                filterArray[i + 1] =
                                    stripQuote(filterArray[i + 1]);
                            }
                            break;
                        }
                    }
                }
            }
            queryString = join(" ", filterArray);
            queryString = query.substring(0, fromIdx + 7).concat(queryString);
        }
        return queryString;
    }

    private static String stripQuote(String s)
    {
        String ret = s;
        if (s.startsWith("'")) {
            int lastQuote = s.lastIndexOf("'");
            ret = s.substring(1, lastQuote);
            if (s.length() > (lastQuote + 1)) {
                ret = ret.concat(s.substring(lastQuote + 1));
            }
        }
        return ret;
    }

    private static String join(String delimiter, String [] array)
    {
        String ret = "";
        for (String s : array) {
            ret = ret.concat(s).concat(delimiter);
        }
        return ret.trim();
    }
}

// End SfdcUdx.java
