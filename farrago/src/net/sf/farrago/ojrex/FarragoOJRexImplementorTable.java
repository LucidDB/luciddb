/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.ojrex;

import org.eigenbase.oj.rex.OJRexImplementorTable;
import org.eigenbase.oj.rex.OJRexImplementorTableImpl;
import org.eigenbase.sql.SqlBinaryOperator;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlPrefixOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;


/**
 * FarragoOJRexImplementorTable implements {@link OJRexImplementorTable} with
 * Farrago-specific translations for standard operators and functions.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexImplementorTable extends OJRexImplementorTableImpl
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a table with all supported standard operators registered.
     */
    public FarragoOJRexImplementorTable(SqlStdOperatorTable opTab)
    {
        initStandard(opTab);
    }

    //~ Methods ---------------------------------------------------------------

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
            opTab.lnFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.LN_FUNCTION));

        registerOperator(
            opTab.log10Func,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.LOG10_FUNCTION));

        registerOperator(
            opTab.absFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.ABS_FUNCTION));

        registerOperator(
            opTab.ceilFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CEIL_FUNCTION));

        registerOperator(
            opTab.floorFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.FLOOR_FUNCTION));

        registerOperator(
            opTab.expFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.EXP_FUNCTION));

        registerOperator(
            opTab.modFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.MOD_FUNCTION));


        registerOperator(
            opTab.substringFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.SUBSTRING_FUNCTION));

        registerOperator(
            opTab.overlayFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.OVERLAY_FUNCTION));

        registerOperator(
            opTab.powFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.POW_FUNCTION));

        registerOperator(
            opTab.concatOperator,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CONCAT_OPERATOR));

        /*
        registerOperator(
            opTab.convertFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CONVERT_FUNCTION));

        registerOperator(
            opTab.translateFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.TRANSLATE_FUNCTION));
         */

        registerOperator(
            opTab.positionFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.POSITION_FUNCTION));

        registerOperator(
            opTab.trimFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.TRIM_FUNCTION));

        registerOperator(
            opTab.charLengthFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CHAR_LENGTH_FUNCTION));

        registerOperator(
            opTab.characterLengthFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.CHARACTER_LENGTH_FUNCTION));

        registerOperator(
            opTab.upperFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.UPPER_FUNCTION));

        registerOperator(
            opTab.lowerFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.LOWER_FUNCTION));

        registerOperator(
            opTab.initcapFunc,
            new FarragoOJRexBuiltinImplementor(
                FarragoOJRexBuiltinImplementor.INITCAP_FUNCTION));

        registerOperator(
            opTab.caseOperator,
            new FarragoOJRexCaseImplementor());

        registerOperator(
            opTab.castFunc,
            new FarragoOJRexCastImplementor());

        registerOperator(
            opTab.isTrueOperator,
            new FarragoOJRexTruthTestImplementor(true));

        registerOperator(
            opTab.isFalseOperator,
            new FarragoOJRexTruthTestImplementor(false));

        registerOperator(
            opTab.isNullOperator,
            new FarragoOJRexNullTestImplementor(true));

        registerOperator(
            opTab.isNotNullOperator,
            new FarragoOJRexNullTestImplementor(false));

        registerOperator(
            opTab.rowConstructor,
            new FarragoOJRexRowImplementor());

        registerOperator(
            opTab.newOperator,
            new FarragoOJRexRowImplementor());

        registerContextOp(opTab.userFunc);
        registerContextOp(opTab.systemUserFunc);
        registerContextOp(opTab.sessionUserFunc);
        registerContextOp(opTab.currentUserFunc);
        registerContextOp(opTab.currentRoleFunc);
        registerContextOp(opTab.currentPathFunc);
        registerContextOp(opTab.currentDateFunc);
        registerContextOp(opTab.currentTimeFunc);
        registerContextOp(opTab.currentTimestampFunc);
        registerContextOp(opTab.localTimeFunc);
        registerContextOp(opTab.localTimestampFunc);
    }

    private void registerContextOp(SqlFunction op)
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
