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
