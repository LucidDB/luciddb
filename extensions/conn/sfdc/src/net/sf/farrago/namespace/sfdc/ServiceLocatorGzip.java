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

import com.sforce.soap.partner.SforceServiceLocator;

import javax.xml.rpc.Call;
import javax.xml.rpc.ServiceException;

import org.apache.axis.transport.http.HTTPConstants;


/**
 * ServiceLocatorGzip allows for compression for sforce data xfers
 *
 * @author boris
 * @version $Id$
 */
public class ServiceLocatorGzip
    extends SforceServiceLocator
{
    //~ Methods ----------------------------------------------------------------

    public Call createCall()
        throws ServiceException
    {
        Call call = super.createCall();
        call.setProperty(HTTPConstants.MC_ACCEPT_GZIP, Boolean.TRUE);
        call.setProperty(HTTPConstants.MC_GZIP_REQUEST, Boolean.TRUE);
        return call;
    }
}
// End ServiceLocatorGzip.java
