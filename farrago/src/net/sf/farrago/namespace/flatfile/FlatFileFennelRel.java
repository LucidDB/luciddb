/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.namespace.flatfile;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FlatFileFennelRel provides a flatfile implementation for {@link
 * TableAccessRel} with {@link FennelRel#FENNEL_EXEC_CONVENTION}.
 *
 * @author John Pham
 * @version $Id$
 */
public class FlatFileFennelRel
    extends TableAccessRelBase
    implements FennelRel
{
    //~ Static fields/initializers ---------------------------------------------

    // name of the session parameter for log directory
    public static final String LOG_DIR = "logDir";

    // max length of text for a row when signalling an error
    // NOTE: keep this consistent with the Fennel file
    //   fennel/flatfile/FlatFileExecStreamImpl.cpp
    public static final int MAX_ROW_ERROR_TEXT_WIDTH = 4000;

    //~ Instance fields --------------------------------------------------------

    private FlatFileColumnSet columnSet;
    private FlatFileParams.SchemaType schemaType;
    FlatFileParams params;

    //~ Constructors -----------------------------------------------------------

    protected FlatFileFennelRel(
        FlatFileColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection,
        FlatFileParams.SchemaType schemaType,
        FlatFileParams params)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            columnSet,
            connection);
        this.columnSet = columnSet;
        this.schemaType = schemaType;
        this.params = params;
    }

    protected FlatFileFennelRel(
        FlatFileColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection,
        FlatFileParams.SchemaType schemaType,
        FlatFileParams params,
        RelDataType rowType)
    {
        this(columnSet, cluster, connection, schemaType, params);
        this.rowType = rowType;
    }

    //~ Methods ----------------------------------------------------------------

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
            program = "sample";
            break;
            // fall through to get program
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

        streamDef.setLenient(params.getLenient());
        streamDef.setTrim(params.getTrim());
        streamDef.setMapped(params.getMapped());
        java.util.List<FemColumnName> columnNames = streamDef.getColumn();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            FemColumnName name = repos.newFemColumnName();
            name.setName(rowType.getFields()[i].getName());
            columnNames.add(name);
        }

        // set the error record type to be a single text column
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);
        RelDataTypeFactory typeFactory =
            stmt.getRelOptCluster().getTypeFactory();
        RelDataType errorText =
            typeFactory.createSqlType(
                SqlTypeName.VARCHAR,
                MAX_ROW_ERROR_TEXT_WIDTH);
        errorText = typeFactory.createTypeWithNullability(errorText, true);
        errorText =
            FlatFileBcpFile.forceSingleByte(
                typeFactory,
                errorText);
        RelDataType errorType =
            typeFactory.createStructType(
                new RelDataType[] { errorText },
                new String[] { "ROW_TEXT" });
        implementor.setErrorRecordType(this, streamDef, errorType);

        return streamDef;
    }

    private String encodeChar(char c)
    {
        return (c == 0) ? "" : Character.toString(c);
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }

    // implement RelNode
    public FlatFileFennelRel clone()
    {
        FlatFileFennelRel clone =
            new FlatFileFennelRel(
                columnSet,
                getCluster(),
                connection,
                schemaType,
                params,
                getRowType());
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End FlatFileFennelRel.java
