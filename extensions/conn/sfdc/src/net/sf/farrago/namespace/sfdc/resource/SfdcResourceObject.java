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
package net.sf.farrago.namespace.sfdc.resource;

/**
 * Contains a singleton instance of {@link SfdcResource} class for default
 * locale. Note: this is a workaround since SfdcResource.instance() has problems
 * loading the bundle from the jar.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class SfdcResourceObject
{
    //~ Static fields/initializers ---------------------------------------------

    private static final SfdcResource res;

    static {
        try {
            res = new SfdcResource();
        } catch (Throwable ex) {
            throw new Error(ex);
        }
    }

    //~ Methods ----------------------------------------------------------------

    public static SfdcResource get()
    {
        return res;
    }
}

// End SfdcResourceObject.java
