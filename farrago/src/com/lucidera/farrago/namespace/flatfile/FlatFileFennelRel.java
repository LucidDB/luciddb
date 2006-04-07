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

import java.io.*;
import java.text.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import com.disruptivetech.farrago.calc.*;

/**
 * FlatFileFennelRel provides a flatfile implementation for
 * {@link TableAccessRel} with {@link FennelRel#FENNEL_EXEC_CONVENTION}.
 *
 * @author John Pham
 * @version $Id$
 */
class FlatFileFennelRel extends TableAccessRelBase implements FennelRel
{
    public static final int FLAT_FILE_MAX_NON_CHAR_VALUE_LEN = 255;
    
    //~ Instance fields -------------------------------------------------------

    private FlatFileColumnSet columnSet;
    FlatFileParams.SchemaType schemaType;

    //~ Constructors ----------------------------------------------------------

    FlatFileFennelRel(
        FlatFileColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection,
        FlatFileParams.SchemaType schemaType)
    {
        super(
            cluster, new RelTraitSet(FENNEL_EXEC_CONVENTION), columnSet,
            connection);
        this.columnSet = columnSet;
        this.schemaType = schemaType;
    }
    
    //~ Methods ---------------------------------------------------------------

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
            ProgramWriter pw = 
                new ProgramWriter(getCluster().getRexBuilder());
            program = pw.write(rowType);
            break;
        default:
            Util.needToImplement("unsupported schema type");
            break;
        }
        boolean header = params.getWithHeader();
        if (numRowsScan > 0 && header) {
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
        return new RelFieldCollation [] { new RelFieldCollation(0) };
    }

    // implement RelNode
    public Object clone()
    {
        FlatFileFennelRel clone = new FlatFileFennelRel(
            columnSet, getCluster(), connection, schemaType);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Constructs a Calculator program for translating text
     * from a flat file into typed data. It is assumed that the text
     * has already been processed for quoting and escape characters.
     */
    private class ProgramWriter 
    {
        RexBuilder rexBuilder;
        RelDataTypeFactory typeFactory;
        
        public ProgramWriter(RexBuilder rexBuilder) 
        {
            this.rexBuilder = rexBuilder;
            typeFactory = rexBuilder.getTypeFactory(); 
        }
        
        /**
         * Given the description of the expected data types,
         * generates a program for converting text into typed data.
         *
         * <p>
         * 
         * First this method infers the description of text columns
         * required to read the exptected data values. Then it
         * constructs the casts necessary to perform data conversion.
         * Date conversions may require special functions.
         *
         * <p>
         *
         * It relies on a {@link RexToCalcTranslator} to convert the
         * casts into a calculator program.
         */
        public String write(RelDataType rowType) 
        {
            assert(rowType.isStruct());
            RelDataTypeField[] targetTypes = rowType.getFields();

            // infer source text types
            RelDataType[] sourceTypes = new RelDataType[targetTypes.length];
            String[] sourceNames = new String[targetTypes.length];
            for (int i = 0; i < targetTypes.length; i++) {
                RelDataType targetType = targetTypes[i].getType();
                sourceTypes[i] = getTextType(targetType);
                sourceNames[i] = targetTypes[i].getName();
            }
            RelDataType inputRowType =
                typeFactory.createStructType(sourceTypes, sourceNames);
            
            // construct rex program
            RexProgramBuilder programBuilder =
                new RexProgramBuilder(inputRowType, rexBuilder);
            for (int i = 0; i < targetTypes.length; i++) {
                RelDataType targetType = targetTypes[i].getType();
                // TODO: call dedicated functions for conversion of dates
                programBuilder.addProject(
                    rexBuilder.makeCast(
                        targetType,
                        programBuilder.makeInputRef(i)),
                    sourceNames[i]);
            }
            RexProgram program = programBuilder.getProgram();

            // translate to a fennel calc program
            RexToCalcTranslator translator =
                new RexToCalcTranslator(rexBuilder);
            return translator.generateProgram(inputRowType, program);
        }

        /**
         * Converts a SQL type into a type that can be used by
         * a Fennel FlatFileExecStream to read files.
         */
        private RelDataType getTextType(RelDataType sqlType) 
        {
            int length = FLAT_FILE_MAX_NON_CHAR_VALUE_LEN;
            switch (sqlType.getSqlTypeName().getOrdinal()) {
            case SqlTypeName.Char_ordinal:
            case SqlTypeName.Varchar_ordinal:
                length = sqlType.getPrecision();
                break;
            case SqlTypeName.Bigint_ordinal:
            case SqlTypeName.Boolean_ordinal:
            case SqlTypeName.Date_ordinal:
            case SqlTypeName.Decimal_ordinal:
            case SqlTypeName.Double_ordinal:
            case SqlTypeName.Float_ordinal:
            case SqlTypeName.Integer_ordinal:
            case SqlTypeName.Real_ordinal:
            case SqlTypeName.Smallint_ordinal:
            case SqlTypeName.Time_ordinal:
            case SqlTypeName.Timestamp_ordinal:
            case SqlTypeName.Tinyint_ordinal:
                break;
            case SqlTypeName.Binary_ordinal:
            case SqlTypeName.IntervalDayTime_ordinal:
            case SqlTypeName.IntervalYearMonth_ordinal:
            case SqlTypeName.Multiset_ordinal:
            case SqlTypeName.Null_ordinal:
            case SqlTypeName.Row_ordinal:
            case SqlTypeName.Structured_ordinal:
            case SqlTypeName.Symbol_ordinal:
            case SqlTypeName.Varbinary_ordinal:
            default:
                // unsupported for flat files
                assert(false) : "Type is unsupported for flat files: " +
                    sqlType.getSqlTypeName();
            }
            return typeFactory.createSqlType(SqlTypeName.Varchar, length);
        }
    }
}


// End FlatFileFennelRel.java
