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
package net.sf.farrago.ojrex;

import openjava.ptree.*;

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
}


// End FarragoOJRexImplementorTable.java
