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

import java.sql.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * MedMockFennelRel provides a mock implementation for
 * {@link TableAccessRel} with {@link FennelPullRel#FENNEL_PULL_CONVENTION}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockFennelRel extends TableAccessRel implements FennelPullRel
{
    //~ Instance fields -------------------------------------------------------

    private MedMockColumnSet columnSet;

    //~ Constructors ----------------------------------------------------------

    MedMockFennelRel(
        MedMockColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        super(cluster, columnSet, connection);
        this.columnSet = columnSet;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public CallingConvention getConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement FennelRel
    public FarragoPreparingStmt getPreparingStmt()
    {
        return columnSet.getPreparingStmt();
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = getPreparingStmt().getRepos();

        FemMockTupleStreamDef streamDef = repos.newFemMockTupleStreamDef();
        streamDef.setRowCount(columnSet.nRows);
        return streamDef;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // trivially sorted
        return new RelFieldCollation [] { new RelFieldCollation(0) };
    }
}


// End MedMockFennelRel.java
