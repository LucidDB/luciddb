/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.ojrex;

import org.eigenbase.oj.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;


/**
 * FarragoOJRexImplementorTable implements {@link OJRexImplementorTable} with
 * Farrago-specific translations for standard operators and functions.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexImplementorTable
    extends OJRexImplementorTableImpl
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a table with all supported standard operators registered.
     */
    public FarragoOJRexImplementorTable(SqlStdOperatorTable opTab)
    {
        initStandard(opTab);
    }

    //~ Methods ----------------------------------------------------------------

    // override OJRexImplementorTableImpl
    protected void initStandard(final SqlStdOperatorTable opTab)
    {
        // use org.eigenbase.oj.rex implementation as a base
        super.initStandard(opTab);

        // NOTE jvs 22-June-2004: when you add implementations for new
        // operators here, please add a corresponding test case in
        // FarragoRexToOJTranslatorTest
        // refine with Farrago specifics
        registerOperator(
            SqlStdOperatorTable.lnFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.LN_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.log10Func,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.LOG10_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.absFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.ABS_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.ceilFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CEIL_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.floorFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.FLOOR_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.expFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.EXP_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.modFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.MOD_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.substringFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.SUBSTRING_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.overlayFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.OVERLAY_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.powFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.POW_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.concatOperator,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CONCAT_OPERATOR));

        /*
        registerOperator( SqlStdOperatorTable.convertFunc, new FarragoOJRexBuiltinImplementor(
         FarragoOJRexBuiltinImplementor.CONVERT_FUNCTION));

         registerOperator( SqlStdOperatorTable.translateFunc, new
         FarragoOJRexBuiltinImplementor(
         FarragoOJRexBuiltinImplementor.TRANSLATE_FUNCTION));
         */

        registerOperator(
            SqlStdOperatorTable.positionFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.POSITION_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.trimFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.TRIM_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.charLengthFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CHAR_LENGTH_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.characterLengthFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CHARACTER_LENGTH_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.upperFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.UPPER_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.lowerFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.LOWER_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.initcapFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.INITCAP_FUNCTION));

        registerOperator(
            SqlStdOperatorTable.similarOperator,
            new FarragoOJRexSimilarLikeImplementor(true, false));

        registerOperator(
            SqlStdOperatorTable.notSimilarOperator,
            new FarragoOJRexSimilarLikeImplementor(true, true));

        registerOperator(
            SqlStdOperatorTable.likeOperator,
            new FarragoOJRexSimilarLikeImplementor(false, false));

        registerOperator(
            SqlStdOperatorTable.notLikeOperator,
            new FarragoOJRexSimilarLikeImplementor(false, true));

        registerOperator(
            SqlStdOperatorTable.caseOperator,
            new FarragoOJRexCaseImplementor());

        registerOperator(
            SqlStdOperatorTable.castFunc,
            new FarragoOJRexCastImplementor());

        registerOperator(
            SqlStdOperatorTable.isTrueOperator,
            new FarragoOJRexTruthTestImplementor(true));

        registerOperator(
            SqlStdOperatorTable.isFalseOperator,
            new FarragoOJRexTruthTestImplementor(false));

        registerOperator(
            SqlStdOperatorTable.isNullOperator,
            new FarragoOJRexNullTestImplementor(true));

        registerOperator(
            SqlStdOperatorTable.isNotNullOperator,
            new FarragoOJRexNullTestImplementor(false));

        registerOperator(
            SqlStdOperatorTable.rowConstructor,
            new FarragoOJRexRowImplementor());

        registerOperator(
            SqlStdOperatorTable.newOperator,
            new FarragoOJRexRowImplementor());

        registerOperator(
            SqlStdOperatorTable.reinterpretOperator,
            new FarragoOJRexReinterpretImplementor());

        registerOperator(
            SqlStdOperatorTable.nextValueFunc,
            new FarragoOJRexNextValueImplementor());

        registerContextOp(SqlStdOperatorTable.userFunc);
        registerContextOp(SqlStdOperatorTable.systemUserFunc);
        registerContextOp(SqlStdOperatorTable.sessionUserFunc);
        registerContextOp(SqlStdOperatorTable.currentUserFunc);
        registerContextOp(SqlStdOperatorTable.currentRoleFunc);
        registerContextOp(SqlStdOperatorTable.currentPathFunc);
        registerContextOp(SqlStdOperatorTable.currentDateFunc);
        registerContextOp(SqlStdOperatorTable.currentTimeFunc);
        registerContextOp(SqlStdOperatorTable.currentTimestampFunc);
        registerContextOp(SqlStdOperatorTable.localTimeFunc);
        registerContextOp(SqlStdOperatorTable.localTimestampFunc);
    }

    protected void registerContextOp(SqlFunction op)
    {
        registerOperator(
            op,
            new FarragoOJRexContextVariableImplementor(op.getName()));
    }

    // override OJRexImplementorTableImpl
    protected void registerBinaryOperator(
        SqlBinaryOperator op,
        int ojBinaryExpressionOrdinal)
    {
        registerOperator(
            op,
            new FarragoOJRexBinaryExpressionImplementor(
                ojBinaryExpressionOrdinal));
    }

    protected void registerUnaryOperator(
        SqlPrefixOperator op,
        int ojUnaryExpressionOrdinal)
    {
        registerOperator(
            op,
            new FarragoOJRexUnaryExpressionImplementor(
                ojUnaryExpressionOrdinal));
    }
}

// End FarragoOJRexImplementorTable.java
