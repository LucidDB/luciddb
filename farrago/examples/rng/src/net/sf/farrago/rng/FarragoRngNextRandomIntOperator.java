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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import net.sf.farrago.query.*;
import net.sf.farrago.cwm.core.*;

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
            ReturnTypeInferenceImpl.useInteger, 
            null,
            new OperandsTypeChecking.SimpleOperandsTypeChecking(
                new SqlTypeName [][] {
                    SqlTypeName.intTypes, 
                    SqlTypeName.charTypes,
                    SqlTypeName.charTypes
                }), 
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
        CwmModelElement rng = 
            preparingStmt.getStmtValidator().findSchemaObject(
                id,
                FarragoRngUDR.getRngModelPackage(
                    preparingStmt.getRepos())
                .getRngschema().getRngRandomNumberGenerator());
        
        preparingStmt.addDependency(rng);
    }
    
    // override SqlOperator
    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.print(getName());
        writer.print("(");
        SqlLiteral ceiling = (SqlLiteral) operands[0];
        if (ceiling.intValue() == -1) {
            writer.print("UNBOUNDED ");
        } else {
            writer.print("CEILING ");
            ceiling.unparse(writer, leftPrec, rightPrec);
        }
        writer.print(" FROM ");
        SqlLiteral id = (SqlLiteral) operands[1];
        writer.print(id.getStringValue());
        writer.print(")");
    }
}

// End FarragoRngNextRandomIntOperator.java
