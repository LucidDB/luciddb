/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.namespace.mock;

import java.sql.*;
import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * MedMockColumnSet provides a mock implementation of the {@link
 * FarragoMedColumnSet} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockColumnSet extends MedAbstractColumnSet
{
    //~ Instance fields -------------------------------------------------------

    final long nRows;
    final String executorImpl;

    //~ Constructors ----------------------------------------------------------

    MedMockColumnSet(
        String [] localName,
        RelDataType rowType,
        long nRows,
        String executorImpl)
    {
        super(localName, null, rowType, null, null);
        this.nRows = nRows;
        this.executorImpl = executorImpl;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptTable
    public double getRowCount()
    {
        return nRows;
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        if (executorImpl.equals(MedMockDataServer.PROPVAL_JAVA)) {
            return new MedMockIterRel(this, cluster, connection);
        } else {
            return new MedMockFennelRel(this, cluster, connection);
        }
    }
}


// End MedMockColumnSet.java
