/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package net.sf.farrago.jdbc;

import java.sql.DriverPropertyInfo;
import org.objectweb.rmijdbc.RJDriverPropertyInfo;

/**
 * Serializable DriverPropertyInfo passed through RMI.
 *
 * <p>This class extends RmiJdbc's RJDriverPropertyInfo
 * which implements {@link java.io.Serializable} interface
 * to pass the DriverPropertyInfo class from server to client via RMI.
 *
 * @author Tim Leung
 * @version $Id$
 **/
public class FarragoRJDriverPropertyInfo extends RJDriverPropertyInfo
    implements java.io.Serializable
{
    /** SerialVersionUID created with JDK 1.5 serialver tool. */
    private static final long serialVersionUID = -5324961535239009568L;

    public FarragoRJDriverPropertyInfo(DriverPropertyInfo dpi)
    {
        super(dpi);
    }
};

// End FarragoRJDriverPropertyInfo.java
