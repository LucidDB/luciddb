/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.namespace.jdbc;

import org.eigenbase.rel.metadata.*;


/**
 * MedJdbcMetadataProvider supplies metadata to the optimizer about JDBC
 * relational expressions.
 *
 * @author John Sichi
 * @version $Id$
 */
public class MedJdbcMetadataProvider
    extends ReflectiveRelMetadataProvider
{
    //~ Methods ----------------------------------------------------------------

    public Boolean canRestart(MedJdbcQueryRel rel)
    {
        // We don't support restarting a JDBC query.  We could do it via scroll
        // cursors, but (a) not all drivers implement scroll cursors, and (b)
        // we wouldn't want to unconditionally request a scroll cursor since it
        // adds overhead.  So, get the optimizer to force the buffering inside
        // of Farrago as needed.
        return false;
    }
}

// End MedJdbcMetadataProvider.java
