/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
//
*/
package org.eigenbase.sql.type;

/**
 * Holds constants associated with SQL types introduced after the earliest
 * version of Java supported by Farrago (this currently means anything
 * introduced in JDK 1.6 or later).
 *
 * Allows us to deal sanely with type constants returned by newer JDBC
 * drivers when running a version of Farrago compiled under an old
 * version of the JDK (i.e. 1.5).
 *
 * By itself, the presence of a constant here doesn't imply that farrago
 * fully supports the associated type.  This is simply a mirror of the
 * missing constant values.
 */
public interface ExtraSqlTypes
{
    // From JDK 1.6
    public final static int ROWID = -8;
    public final static int NCHAR = -15;
    public final static int NVARCHAR = -9;
    public final static int LONGNVARCHAR = -16;
    public final static int NCLOB = 2011;
    public final static int SQLXML = 2009;
}

// End ExtraSqlTypes.java