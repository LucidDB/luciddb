/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.rng;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import net.sf.farrago.query.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.rngmodel.rngschema.RngRandomNumberGenerator;

/**
 * FarragoRngNextRandomIntOperator defines the SqlOperator for the
 * NEXT_RANDOM_INT pseudo-function.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRngNextRandomIntOperator extends SqlFunction
{
    public FarragoRngNextRandomIntOperator()
    {
        super(
            "NEXT_RANDOM_INT",
            SqlKind.Other,
            SqlTypeStrategies.rtiInteger,
            null,
            new FamilyOperandTypeChecker(
                SqlTypeFamily.INTEGER,
                SqlTypeFamily.CHARACTER,
                SqlTypeFamily.CHARACTER),
            SqlFunctionCategory.System);
    }

    // override SqlOperator
    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        super.validateCall(call, validator, scope, operandScope);

        // TODO jvs 18-Apr-2005: make this an official part of session-level
        // interface
        FarragoPreparingStmt preparingStmt =
            (FarragoPreparingStmt) validator.getCatalogReader();

        // This is kind of silly...we already had the rng reference
        // during parsing, but then we lost it.
        SqlParser sqlParser = new SqlParser(
            ((SqlLiteral) call.operands[1]).getStringValue());
        SqlIdentifier id;
        try {
            id = (SqlIdentifier) sqlParser.parseExpression();
        } catch (Throwable ex) {
            throw Util.newInternal(ex);
        }
        RngRandomNumberGenerator rng =
            preparingStmt.getStmtValidator().findSchemaObject(
                id,
                RngRandomNumberGenerator.class);

        // TODO jvs 27-Aug-2005:  make this USAGE instead
        preparingStmt.addDependency(rng, PrivilegedActionEnum.REFERENCES);
    }

    // override SqlOperator
    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        SqlLiteral ceiling = (SqlLiteral) operands[0];
        if (ceiling.intValue(true) == -1) {
            writer.sep("UNBOUNDED");
        } else {
            writer.sep("CEILING");
            ceiling.unparse(writer, leftPrec, rightPrec);
        }
        writer.sep("FROM");
        SqlLiteral id = (SqlLiteral) operands[1];
        writer.literal(id.getStringValue());
        writer.endFunCall(frame);
    }
}

// End FarragoRngNextRandomIntOperator.java
