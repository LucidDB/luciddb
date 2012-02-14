/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
            SqlKind.OTHER,
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
