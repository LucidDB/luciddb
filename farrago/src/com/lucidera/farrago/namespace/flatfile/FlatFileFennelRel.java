/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.farrago.namespace.flatfile;

import java.sql.*;
import java.util.*;

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
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * FlatFileFennelRel provides a flatfile implementation for
 * {@link TableAccessRel} with {@link FennelRel#FENNEL_EXEC_CONVENTION}.
 *
 * @author John V. Pham
 * @version $Id$
 */
class FlatFileFennelRel extends TableAccessRelBase implements FennelRel
{
    //~ Instance fields -------------------------------------------------------

    private FlatFileColumnSet columnSet;

    //~ Constructors ----------------------------------------------------------

    FlatFileFennelRel(
        FlatFileColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        super(
            cluster, new RelTraitSet(FENNEL_EXEC_CONVENTION), columnSet,
            connection);
        this.columnSet = columnSet;
    }

    //~ Methods ---------------------------------------------------------------

    private String constructCalcProgram(RelDataType rowType) 
    {
        // TODO: data conversion
        return null;
    }
        
    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FlatFileParams params = columnSet.getParams();
        
        FemFlatFileTupleStreamDef streamDef =
            repos.newFemFlatFileTupleStreamDef();
        streamDef.setDataFilePath(columnSet.getFilename());
        if (params.getWithErrorLogging()) {
            // TODO: log errors to file
            //streamDef.setErrorFilePath();
        }
        streamDef.setHasHeader(params.getWithHeader());
        streamDef.setNumRowsScan(params.getNumRowsScan());
        streamDef.setFieldDelimiter(
            Character.toString(params.getFieldDelimiter()));
        streamDef.setRowDelimiter(
            Character.toString(params.getLineDelimiter()));
        streamDef.setQuoteCharacter(
            Character.toString(params.getQuoteChar()));
        streamDef.setEscapeCharacter(
            Character.toString(params.getEscapeChar()));
        streamDef.setCalcProgram(
            constructCalcProgram(columnSet.getRowType()));
        
        return streamDef;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // trivially sorted
        return new RelFieldCollation [] { new RelFieldCollation(0) };
    }

    // implement RelNode
    public Object clone()
    {
        FlatFileFennelRel clone =
            new FlatFileFennelRel(columnSet, getCluster(), connection);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}


// End FlatFileFennelRel.java
