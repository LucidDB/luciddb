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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * FlatFileFennelRel provides a flatfile implementation for {@link
 * TableAccessRel} with {@link FennelRel#FENNEL_EXEC_CONVENTION}.
 *
 * @author John Pham
 * @version $Id$
 */
class FlatFileFennelRel
    extends TableAccessRelBase
    implements FennelRel
{

    //~ Instance fields --------------------------------------------------------

    private FlatFileColumnSet columnSet;
    FlatFileParams.SchemaType schemaType;

    //~ Constructors -----------------------------------------------------------

    protected FlatFileFennelRel(
        FlatFileColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection,
        FlatFileParams.SchemaType schemaType)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            columnSet,
            connection);
        this.columnSet = columnSet;
        this.schemaType = schemaType;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Determines whether the flatfile scan can be implemented entirely within
     * this Fennel RelNode. If not, then it requires a Java program.
     */
    public boolean isPureFennel()
    {
        // Use only FennelRel in basic modes, or if
        // the Fennel calc mode is turned on
        CalcVirtualMachine calcVm =
            FennelRelUtil.getPreparingStmt(this).getRepos().getCurrentConfig()
            .getCalcVirtualMachine();
        if ((schemaType == FlatFileParams.SchemaType.DESCRIBE)
            || (schemaType == FlatFileParams.SchemaType.SAMPLE)
            || calcVm.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
            return true;
        }
        return false;
    }

    /**
     * Forces a flatfile scan to run in text only mode, bypassing the casting of
     * columns into typed data. This call is only valid for regular queries (not
     * describe or sample).
     *
     * @param textRowType target row type for the scan, all columns must be
     * character columns.
     */
    public void setTextOnly(RelDataType textRowType)
    {
        assert (schemaType == FlatFileParams.SchemaType.QUERY);
        schemaType = FlatFileParams.SchemaType.QUERY_TEXT;
        rowType = textRowType;
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
        streamDef.setDataFilePath(columnSet.getFilePath());
        if (params.getWithLogging()) {
            streamDef.setErrorFilePath(columnSet.getLogFilePath());
        }
        streamDef.setFieldDelimiter(encodeChar(params.getFieldDelimiter()));
        streamDef.setRowDelimiter(encodeChar(params.getLineDelimiter()));
        streamDef.setQuoteCharacter(encodeChar(params.getQuoteChar()));
        streamDef.setEscapeCharacter(encodeChar(params.getEscapeChar()));

        // The schema type is encoded into the number of rows to
        // scan and program parameters
        int numRowsScan = 0;
        String program = "";
        switch (schemaType) {
        case DESCRIBE:
            numRowsScan = params.getNumRowsScan();
            break;
        case SAMPLE:
            numRowsScan = params.getNumRowsScan();
            // fall through to get program
        case QUERY:
            FlatFileProgramWriter pw = new FlatFileProgramWriter(this);
            RexProgram rexProgram = pw.getProgram(rowType);
            program = pw.toFennelProgram(rexProgram);
            break;
        case QUERY_TEXT:
            break;
        default:
            Util.needToImplement("unsupported schema type");
            break;
        }
        boolean header = params.getWithHeader();
        if ((numRowsScan > 0) && header) {
            // during a sample or describe query, treat header as any
            // other data, but do not count it against the user specified
            // number of rows to scan
            numRowsScan++;
            header = false;
        }
        streamDef.setNumRowsScan(numRowsScan);
        streamDef.setCalcProgram(program);
        streamDef.setHasHeader(header);

        // TODO: get/set from server options parameters
        streamDef.setSubstituteCharacter("?");
        streamDef.setCodePage(0);
        streamDef.setTranslationRecovery(true);

        return streamDef;
    }

    public String encodeChar(char c)
    {
        return (c == 0) ? "" : Character.toString(c);
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // REVIEW jvs 18-Feb-2006:  how so?

        // trivially sorted
        return new RelFieldCollation[] { new RelFieldCollation(0) };
    }

    // implement RelNode
    public Object clone()
    {
        FlatFileFennelRel clone =
            new FlatFileFennelRel(
                columnSet,
                getCluster(),
                connection,
                schemaType);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End FlatFileFennelRel.java
