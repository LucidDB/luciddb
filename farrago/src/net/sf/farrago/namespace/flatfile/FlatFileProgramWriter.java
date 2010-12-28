/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import net.sf.farrago.query.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FlatFileProgramWriter builds calculator programs for converting text from
 * flat files into typed data. It assumes the text has already been processed
 * for quoting and escape characters. The programs mainly consist of simple
 * casts. However, custom date formats may be specified when using LucidDb. They
 * are implemented with calls to LucidDb user defined routines. Only one set of
 * formats can be active for a writer.
 *
 * <p>When using custom date conversions, the program can be split into Fennel
 * and Java-only sections. If splitting the program, data must flow through the
 * Fennel section first, which handles basic casts. Then the Java-only section
 * handles custom date conversions.
 *
 * @author jpham
 * @version $Id$
 */
public class FlatFileProgramWriter
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int FLAT_FILE_MAX_NON_CHAR_VALUE_LEN = 255;

    private static final SqlIdentifier toDateFuncName =
        new SqlIdentifier(
            new String[] { "SYS_BOOT", "MGMT", "CHAR_TO_DATE" },
            new SqlParserPos(0, 0));
    private static final SqlIdentifier toTimeFuncName =
        new SqlIdentifier(
            new String[] { "SYS_BOOT", "MGMT", "CHAR_TO_TIME" },
            new SqlParserPos(0, 0));
    private static final SqlIdentifier toTimestampFuncName =
        new SqlIdentifier(
            new String[] { "SYS_BOOT", "MGMT", "CHAR_TO_TIMESTAMP" },
            new SqlParserPos(0, 0));

    private static final int DATETIME_FORMAT_ARG_LENGTH = 50;

    //~ Enums ------------------------------------------------------------------

    /**
     * Identifies a section of a flat file program
     */
    private enum Section
    {
        /**
         * Fennel compatible program section, may include the entire program,
         * but does not include custom datetime conversions.
         */
        FENNEL,

        /**
         * Fennel incompatible program section. Includes custom datetime
         * conversions, but not ISO datetime conversions. May be empty.
         */
        JAVA_ONLY,

        /**
         * Includes the entire program
         */
        ALL
    }

    //~ Instance fields --------------------------------------------------------

    private final RexBuilder rexBuilder;
    private final RelDataTypeFactory typeFactory;
    private final FarragoPreparingStmt stmt;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final RelDataType [] datetimeArgs;
    private final RelDataType rowType;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new FlatFileProgramWriter
     *
     * @param rexBuilder a rex node builder
     * @param stmt the statement being prepared
     * @param params a set of flat file server parameters. These parameters may
     * include custom date formats
     * @param rowType the desired data type for data read from a flat file. The
     * row type is also used to infer the size of text fields in the file.
     */
    public FlatFileProgramWriter(
        RexBuilder rexBuilder,
        FarragoPreparingStmt stmt,
        FlatFileParams params,
        RelDataType rowType)
    {
        this.rexBuilder = rexBuilder;
        typeFactory = rexBuilder.getTypeFactory();
        this.stmt = stmt;
        dateFormat = params.getDateFormat();
        timeFormat = params.getTimeFormat();
        timestampFormat = params.getTimestampFormat();
        datetimeArgs = new RelDataType[2];
        this.rowType = rowType;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Gets the entire data conversion program
     */
    public RexProgram getProgram()
    {
        return getSection(Section.ALL);
    }

    /**
     * Gets the Fennel portion of the data conversion program
     */
    public RexProgram getFennelSection()
    {
        return getSection(Section.FENNEL);
    }

    /**
     * Gets the Java only portion of the data conversion program
     *
     * @return the program, or null if it is empty
     */
    public RexProgram getJavaOnlySection()
    {
        return getSection(Section.JAVA_ONLY);
    }

    /**
     * Builds a section of the data conversion program as follows.
     *
     * <ul>
     * <li>FENNEL: all inputs are text columns. Most inputs are casted to target
     * types, except custom date fields, which are projected
     * <li>JAVA_ONLY: most inputs are typed data, except custom date fields,
     * which are text columns. The typed fields are projected, while the date
     * fields are converted with user defined routines.
     * <li>ALL: all inputs are text columns and all are converted to target
     * types, either by casting or by UDRs.
     * </ul>
     *
     * @param section section of the program to build
     *
     * @return the program section
     */
    private RexProgram getSection(
        Section section)
    {
        assert (rowType.isStruct());
        RelDataTypeField [] targetTypes = rowType.getFields();

        // check for empty section
        if (section == Section.JAVA_ONLY) {
            boolean hasCustom = false;
            for (int i = 0; i < targetTypes.length; i++) {
                if (isCustom(targetTypes[i].getType())) {
                    hasCustom = true;
                    break;
                }
            }
            if (!hasCustom) {
                return null;
            }
        }

        // infer source text types
        RelDataType [] sourceTypes = new RelDataType[targetTypes.length];
        String [] sourceNames = new String[targetTypes.length];
        for (int i = 0; i < targetTypes.length; i++) {
            RelDataType targetType = targetTypes[i].getType();
            if ((section == Section.JAVA_ONLY)
                && (!isCustom(targetType)))
            {
                sourceTypes[i] = targetType;
            } else {
                sourceTypes[i] = getTextType(targetType);
            }
            sourceTypes[i] =
                FlatFileBcpFile.forceSingleByte(
                    typeFactory,
                    sourceTypes[i]);
            sourceNames[i] = targetTypes[i].getName();
        }
        RelDataType inputRowType =
            typeFactory.createStructType(sourceTypes, sourceNames);

        // construct rex program
        RexProgramBuilder programBuilder =
            new RexProgramBuilder(inputRowType, rexBuilder);
        for (int i = 0; i < targetTypes.length; i++) {
            RelDataType targetType = targetTypes[i].getType();
            boolean isCustom = isCustom(targetType);

            boolean skip = false;
            switch (section) {
            case FENNEL:
                skip = isCustom;
                break;
            case JAVA_ONLY:
                if (!isCustom) {
                    if (sourceTypes[i] != targetTypes[i]) {
                        skip = false;
                    } else {
                        skip = true;
                    }
                }
                break;
            }

            RexNode input = programBuilder.makeInputRef(i);
            RexNode node;
            if (skip) {
                // simple projection
                node = input;
            } else if (isCustom) {
                // custom user defined routine
                node =
                    rexBuilder.makeCall(
                        getUdr(targetType, sourceTypes[i]),
                        rexBuilder.makeLiteral(getFormat(targetType)),
                        input);
            } else {
                // simple cast
                node =
                    rexBuilder.makeCast(
                        targetType,
                        input);
            }
            programBuilder.addProject(node, sourceNames[i]);
        }
        return programBuilder.getProgram();
    }

    /**
     * Converts a SQL type into a type that can be used by a Fennel
     * FlatFileExecStream to read files.
     */
    private RelDataType getTextType(RelDataType sqlType)
    {
        int length = FLAT_FILE_MAX_NON_CHAR_VALUE_LEN;
        switch (sqlType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
            length = sqlType.getPrecision();
            break;
        case BIGINT:
        case BOOLEAN:
        case DATE:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case INTEGER:
        case REAL:
        case SMALLINT:
        case TIME:
        case TIMESTAMP:
        case TINYINT:
            break;
        case BINARY:
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
        case MULTISET:
        case NULL:
        case ROW:
        case STRUCTURED:
        case SYMBOL:
        case VARBINARY:
        default:

            // unsupported for flat files
            assert (false) : "Type is unsupported for flat files: "
                + sqlType.getSqlTypeName();
        }
        RelDataType type =
            typeFactory.createSqlType(SqlTypeName.VARCHAR, length);
        return typeFactory.createTypeWithNullability(type, true);
    }

    /**
     * Whether a data type has a custom format
     */
    private boolean isCustom(
        RelDataType type)
    {
        return getFormat(type) != null;
    }

    /**
     * Returns the custom format for a data type
     */
    private String getFormat(RelDataType type)
    {
        switch (type.getSqlTypeName()) {
        case DATE:
            return dateFormat;
        case TIME:
            return timeFormat;
        case TIMESTAMP:
            return timestampFormat;
        default:
            return null;
        }
    }

    /**
     * Gets a function that converts a string to another type, according to a
     * format string. Throws an exception if the function was not found.
     *
     * @param type the target type
     * @param charType the type of the string
     *
     * @return a matching conversion function
     */
    private SqlFunction getUdr(
        RelDataType type,
        RelDataType charType)
    {
        SqlOperatorTable opTable = stmt.getSqlOperatorTable();

        SqlIdentifier funcName;
        switch (type.getSqlTypeName()) {
        case DATE:
            funcName = toDateFuncName;
            break;
        case TIME:
            funcName = toTimeFuncName;
            break;
        case TIMESTAMP:
            funcName = toTimestampFuncName;
            break;
        default:
            funcName = null;
            Util.permAssert(false, "invalid datetime type");
        }

        if (datetimeArgs[0] == null) {
            datetimeArgs[0] =
                typeFactory.createSqlType(
                    SqlTypeName.VARCHAR,
                    DATETIME_FORMAT_ARG_LENGTH);
        }
        datetimeArgs[1] = charType;

        SqlFunction func =
            SqlUtil.lookupRoutine(
                opTable,
                funcName,
                datetimeArgs,
                null);
        Util.permAssert(func != null, "datetime routine not found");
        return func;
    }
}

// End FlatFileProgramWriter.java
