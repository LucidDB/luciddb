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
package net.sf.farrago.namespace.sfdc;

import com.sforce.soap.partner.*;
import com.sforce.soap.partner.fault.*;

import java.net.URL;

import java.rmi.RemoteException;

import java.sql.SQLException;

import java.util.*;

import javax.xml.rpc.ServiceException;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.MedAbstractDataServer;
import net.sf.farrago.namespace.sfdc.resource.*;
import net.sf.farrago.type.FarragoTypeFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * SfdcDataServer provides an implementation of the {@link FarragoMedDataServer}
 * interface.
 *
 * @author Sunny Choi
 * @version $Id$
 */
class SfdcDataServer
    extends MedAbstractDataServer
{
    //~ Static fields/initializers ---------------------------------------------

    // ~ Static fields/initializers --------------------------------------------

    public static final String PROP_USER_NAME = "USER_NAME";
    public static final String PROP_PASSWORD = "PASSWORD";
    public static final String PROP_EXTRA_VARCHAR_PRECISION =
        "VARCHAR_FIELD_EXTRA_PRECISION";
    public static final String PROP_ENDPOINT_URL = "ENDPOINT_URL";

    protected static final int DEFAULT_EXTRA_VARCHAR_PRECISION = 128;
    protected static final Log log = LogFactory.getLog(SfdcDataServer.class);

    //~ Instance fields --------------------------------------------------------

    // ~ Instance fields -------------------------------------------------------

    String username;
    String password;
    int varcharPrecision = DEFAULT_EXTRA_VARCHAR_PRECISION;
    String endpoint;
    SoapBindingStub binding;

    private Date nextLoginTime;

    //~ Constructors -----------------------------------------------------------

    // ~ Constructors ----------------------------------------------------------

    SfdcDataServer(String serverMofId, Properties props)
    {
        super(serverMofId, props);
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods ---------------------------------------------------------------

    void initialize()
        throws SQLException
    {
        Properties props = getProperties();
        this.username = props.getProperty(PROP_USER_NAME);
        this.password = props.getProperty(PROP_PASSWORD);
        this.endpoint = props.getProperty(PROP_ENDPOINT_URL);
        try {
            int precision =
                Integer.parseInt(
                    props.getProperty(PROP_EXTRA_VARCHAR_PRECISION));
            if (precision >= 0) {
                this.varcharPrecision = precision;
            }
        } catch (NumberFormatException ne) {
            // ignore
        }
        nextLoginTime = new Date();
        login(this.username, this.password, null);
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return new SfdcNameDirectory(this, FarragoMedMetadataQuery.OTN_SCHEMA);
    }

    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map columnPropMap)
        throws SQLException
    {
        String objectName = tableProps.getProperty(SfdcColumnSet.PROP_OBJECT);
        if (objectName == null) {
            objectName = getObjectName(localName);
        }

        String updatedFields = null;
        String updatedTypes = null;
        RelDataType srcRowType = deriveRowType(typeFactory, objectName);
        RelDataType mappedRowType = srcRowType;

        if (rowType == null) {
            rowType = mappedRowType;
        } else {
            if (mappedRowType != null) {
                Object [] updatedRowType =
                    updateRowType(
                        typeFactory,
                        rowType,
                        mappedRowType);
                mappedRowType = (RelDataType) updatedRowType[0];
                updatedFields = (String) updatedRowType[1];
                updatedTypes = (String) updatedRowType[2];
            }
        }

        if (mappedRowType == null) {
            return null;
        }

        boolean getAllFields = false;
        String fields = null;
        if (updatedFields == null) {
            fields = getFields(objectName);
        } else {
            String [] updatedFieldsArray = updatedFields.split(",");
            if (!SfdcPushDownRule.validProjection(updatedFieldsArray)) {
                getAllFields = true;
                fields = getFields(objectName);
                mappedRowType = deriveRowType(typeFactory, objectName);
            } else {
                fields = updatedFields;
            }
        }

        String types = null;
        if ((updatedTypes == null) || getAllFields) {
            types = getTypes(objectName, typeFactory);
        } else {
            types = updatedTypes;
        }
        return new SfdcColumnSet(
            this,
            localName,
            mappedRowType,
            rowType,
            srcRowType,
            objectName,
            fields,
            types);
    }

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param)
        throws SQLException
    {
        // minimum session timeout is 30 minutes.
        // Re-login every 20 minutes.
        if (param == null) {
            Date now = new Date();
            if ((this.binding == null)
                || (nextLoginTime.getTime() < now.getTime()))
            {
                login(this.username, this.password, null);
            }
            return this.binding;
        } else {
            if (param instanceof SoapBindingStub) {
                login(this.username, this.password, (SoapBindingStub) param);
                return this.binding;
            }
            return this;
        }
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);

        // delete rules
        planner.addRule(
            new SfdcDeleteRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    SfdcUdxRel.class)
                            })
                    }),
                "filter on proj"));

        planner.addRule(
            new SfdcDeleteRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            SfdcUdxRel.class)
                    }),
                "filter"));

        // pushdown rules

        // case 1: projection on top of a filter (with push down projection)
        // ie: filtering on variables which are not in projection
        planner.addRule(
            new SfdcPushDownRule(
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            FilterRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    ProjectRel.class,
                                    new RelOptRuleOperand[] {
                                        new RelOptRuleOperand(
                                            SfdcUdxRel.class)
                                    })
                            })
                    }),
                "proj on filter on proj"));

        // case 2: filter with push down projection
        // ie: proj only has values which are already in filter expression
        planner.addRule(
            new SfdcPushDownRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    SfdcUdxRel.class)
                            })
                    }),
                "filter on proj"));

        // case 3: filter with no projection to push down.
        // ie: select *
        planner.addRule(
            new SfdcPushDownRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            SfdcUdxRel.class)
                    }),
                "filter"));

        // case 4: only projection, no filter
        planner.addRule(
            new SfdcPushDownRule(
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            SfdcUdxRel.class)
                    }),
                "proj"));
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        super.closeAllocation();
    }

    RelDataType createRowType(
        FarragoTypeFactory typeFactory,
        RelDataType [] types,
        String [] names)
    {
        return typeFactory.createStructType(types, names);
    }

    public String getClientIdFromJNDI()
    // throws NamingException
    {
        String clientId = "";

        /*
         * Context initCtx = new InitialContext(); try { Context envCtx =
         * (Context) initCtx.lookup(LeJndiNames.LE_JNDI_LOOKUP_NAME); clientId =
         * (String) envCtx.lookup(LeJndiNames.SFDC_CLIENT_ID); } catch
         * (NamingException e) { log.warn("Unable to read " +
         * LeJndiNames.SFDC_CLIENT_ID +
         * " from JNDI.  This had better be a development environment."); }
         */
        return clientId;
    }

    private void login(String user, String pass, SoapBindingStub bStub)
    {
        if (bStub != null) {
            this.binding = bStub;
        } else {
            try {
                if ((this.endpoint != null)
                    && !this.endpoint.trim().equals(""))
                {
                    this.binding =
                        (SoapBindingStub)
                        new ServiceLocatorGzip().getSoap(
                            new URL(
                                this.endpoint));
                } else {
                    this.binding =
                        (SoapBindingStub) new ServiceLocatorGzip().getSoap();
                }
            } catch (ServiceException se) {
                throw SfdcResource.instance().SfdcBinding_ServiceException.ex(
                    se.getMessage());
            } catch (Exception ex) {
                log.error("Error logging into SFDC", ex);
                throw SfdcResource.instance().SfdcLoginFault.ex(ex.toString());
            }
        }

        // Set timeout 2 hours
        this.binding.setTimeout(1000 * 60 * 60 * 2);

        LoginResult loginResult = null;
        try {
            /*
             * EncryptDecryptUtil encryptDecryptUtil =
             * EncryptDecryptUtil.getInstance(); // no encryption during test if
             * (encryptDecryptUtil.isEncrypted(pass)) {
             * encryptDecryptUtil.initKeyFromJNDI(); pass =
             * encryptDecryptUtil.decrypt(pass); } if
             * (encryptDecryptUtil.isEncrypted(user)) {
             * encryptDecryptUtil.initKeyFromJNDI(); user =
             * encryptDecryptUtil.decrypt(user); }
             */
            // AppExchange API ClientID
            // CallOptions co = new CallOptions();
            // co.setClient(getClientIdFromJNDI());
            // binding.setHeader(new SforceServiceLocator().getServiceName()
            // .getNamespaceURI(), "CallOptions", co);

            loginResult = this.binding.login(user, pass);
            log.info(SfdcResource.instance().LoggedInMsg.str());
        } catch (LoginFault lf) {
            throw SfdcResource.instance().SfdcLoginFault.ex(
                lf.getExceptionMessage());
        } catch (UnexpectedErrorFault uef) {
            throw SfdcResource.instance().SfdcLoginFault.ex(
                uef.getExceptionMessage());
        } catch (RemoteException re) {
            throw SfdcResource.instance().SfdcLoginFault.ex(re.toString());
        } catch (Exception ex) {
            log.error("Error logging into SFDC", ex);
            throw SfdcResource.instance().SfdcLoginFault.ex(ex.toString());
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
        Date now = new Date();
        nextLoginTime = new Date(now.getTime() + (20 * 60 * 1000));
    }

    private String getFields(String objectName)
        throws SQLException
    {
        if (objectName.endsWith("_LOV") || objectName.endsWith("_deleted")) {
            return null;
        }
        String allFieldNames = null;
        try {
            DescribeSObjectResult describeSObjectResult =
                getEntityDescribe(objectName);

            // check the name
            if ((describeSObjectResult != null)
                && describeSObjectResult.getName().equals(objectName))
            {
                com.sforce.soap.partner.Field [] fields =
                    describeSObjectResult.getFields();
                for (int i = 0; i < fields.length; i++) {
                    if (i == 0) {
                        allFieldNames = fields[i].getName();
                    } else {
                        allFieldNames =
                            allFieldNames + ","
                            + fields[i].getName();
                    }
                }
            } else {
                throw SfdcResource.instance().InvalidObjectException.ex(
                    objectName);
            }
        } catch (InvalidSObjectFault io) {
            throw SfdcResource.instance().InvalidObjectException.ex(
                objectName);
        } catch (RemoteException re) {
            throw SfdcResource.instance().QueryException.ex(
                objectName,
                re.toString());
        }
        return allFieldNames;
    }

    private String getTypes(String objectName, FarragoTypeFactory typeFactory)
        throws SQLException
    {
        if (objectName.endsWith("_LOV") || objectName.endsWith("_deleted")) {
            return null;
        }
        String [] fieldNames = null;
        String types = null;
        try {
            DescribeSObjectResult describeSObjectResult =
                getEntityDescribe(objectName);

            // check the name
            if ((describeSObjectResult != null)
                && describeSObjectResult.getName().equals(objectName))
            {
                com.sforce.soap.partner.Field [] allFields =
                    describeSObjectResult.getFields();
                fieldNames = new String[allFields.length];
                for (int i = 0; i < allFields.length; i++) {
                    fieldNames[i] = allFields[i].getName();
                    if (i == 0) {
                        types =
                            ((SfdcNameDirectory) getNameDirectory()).toRelType(
                                allFields[i],
                                typeFactory).toString();
                    } else {
                        types =
                            types
                            + ","
                            + ((SfdcNameDirectory) getNameDirectory())
                            .toRelType(
                                allFields[i],
                                typeFactory).toString();
                    }
                }
            } else {
                throw SfdcResource.instance().InvalidObjectException.ex(
                    objectName);
            }
        } catch (InvalidSObjectFault io) {
            throw SfdcResource.instance().InvalidObjectException.ex(
                objectName);
        } catch (RemoteException re) {
            throw SfdcResource.instance().QueryException.ex(
                objectName,
                re.toString());
        }
        return types;
    }

    private RelDataType deriveRowType(
        FarragoTypeFactory typeFactory,
        String objectName)
        throws SQLException
    {
        String [] fieldNames = null;
        RelDataType [] types = null;
        try {
            if (objectName.endsWith("_deleted")) {
                types =
                    new RelDataType[] {
                        typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(
                                SqlTypeName.VARCHAR,
                                25 + this.varcharPrecision),
                            true),
                        typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(
                                SqlTypeName.TIMESTAMP,
                                SqlTypeName.TIMESTAMP.getDefaultPrecision()),
                            true)
                    };
                fieldNames = new String[] { "Id", "DeleteStamp" };
            } else if (objectName.endsWith("_LOV")) {
                types =
                    new RelDataType[] {
                        typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(
                                SqlTypeName.VARCHAR,
                                25 + this.varcharPrecision),
                            true),
                        typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(
                                SqlTypeName.VARCHAR,
                                SfdcNameDirectory.MAX_PRECISION
                                + this.varcharPrecision),
                            true)
                    };
                fieldNames = new String[] { "Field", "Value" };
            } else {
                DescribeSObjectResult describeSObjectResult =
                    getEntityDescribe(objectName);

                // check the name
                if ((describeSObjectResult != null)
                    && describeSObjectResult.getName().equals(objectName))
                {
                    com.sforce.soap.partner.Field [] fields =
                        describeSObjectResult.getFields();
                    fieldNames = new String[fields.length];
                    types = new RelDataType[fields.length];
                    for (int i = 0; i < fields.length; i++) {
                        fieldNames[i] = fields[i].getName();
                        types[i] =
                            ((SfdcNameDirectory) getNameDirectory()).toRelType(
                                fields[i],
                                typeFactory);
                    }
                } else {
                    throw SfdcResource.instance().InvalidObjectException.ex(
                        objectName);
                }
            }
        } catch (InvalidSObjectFault io) {
            throw SfdcResource.instance().InvalidObjectException.ex(
                objectName);
        } catch (RemoteException re) {
            throw SfdcResource.instance().QueryException.ex(
                objectName,
                re.toString());
        }

        return createRowType(typeFactory, types, fieldNames);
    }

    private Object [] updateRowType(
        FarragoTypeFactory typeFactory,
        RelDataType currRowType,
        RelDataType srcRowType)
    {
        String fieldNames = "";
        String typeNames = "";
        ArrayList fieldsVector = new ArrayList();
        ArrayList typesVector = new ArrayList();

        HashMap<String, RelDataType> srcMap = new HashMap();
        for (RelDataTypeField srcField : srcRowType.getFieldList()) {
            srcMap.put(srcField.getName(), srcField.getType());
        }

        for (RelDataTypeField currField : currRowType.getFieldList()) {
            RelDataType type;
            if (((type = srcMap.get(currField.getName())) != null)
                && SqlTypeUtil.canCastFrom(currField.getType(), type, true))
            {
                if (fieldNames.equals("")) {
                    fieldNames = currField.getName();
                    typeNames = type.toString();
                } else {
                    fieldNames =
                        fieldNames.concat(",").concat(
                            currField.getName());
                    typeNames = typeNames.concat(",").concat(type.toString());
                }
                fieldsVector.add(currField.getName());
                typesVector.add(type);
            }
        }

        typesVector.trimToSize();
        fieldsVector.trimToSize();
        RelDataType [] types =
            (RelDataType []) typesVector.toArray(
                new RelDataType[typesVector.size()]);
        String [] fields =
            (String []) fieldsVector.toArray(new String[fieldsVector.size()]);

        RelDataType rowType = createRowType(typeFactory, types, fields);
        return new Object[] { rowType, fieldNames, typeNames };
    }

    private String getObjectName(String [] localName)
    {
        assert (localName.length == 3);
        return localName[localName.length - 1];
    }

    /**
     * method to get all SObjects
     */
    public DescribeGlobalResult getEntityTypes()
        throws RemoteException, SQLException
    {
        return ((SoapBindingStub) getRuntimeSupport(null)).describeGlobal();
    }

    /**
     * method describing SObjects
     */
    public DescribeSObjectResult getEntityDescribe(String type)
        throws RemoteException, SQLException
    {
        return ((SoapBindingStub) getRuntimeSupport(null)).describeSObject(
            type);
    }

    public int getVarcharPrecision()
    {
        return this.varcharPrecision;
    }
}

// End SfdcDataServer.java
