/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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

        // NOTE jvs 22-June-2005: when you add implementations for new
        // operators here, please add a corresponding test case in
        // FarragoRexToOJTranslatorTest
        // refine with Farrago specifics
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
            new FarragoOJRexContextVariableImplementor(op.name));
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
