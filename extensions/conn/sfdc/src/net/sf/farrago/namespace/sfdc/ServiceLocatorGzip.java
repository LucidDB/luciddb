/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
