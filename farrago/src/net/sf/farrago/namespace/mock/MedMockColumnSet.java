/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.namespace.mock;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import java.sql.*;
import java.util.*;

/**
 * MedMockColumnSet provides a mock implementation of the {@link
 * FarragoMedColumnSet} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockColumnSet extends MedAbstractColumnSet
{
    final long nRows;

    final String executorImpl;
    
    MedMockColumnSet(
        String [] localName,
        RelDataType rowType,
        long nRows,
        String executorImpl)
    {
        super(
            localName,
            null,
            rowType,
            null,
            null);
        this.nRows = nRows;
        this.executorImpl = executorImpl;
    }
    
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
            return new MedMockIterRel(
                this,
                cluster,
                connection);
        } else {
            return new MedMockFennelRel(
                this,
                cluster,
                connection);
        }
    }
}

// End MedMockColumnSet.java
