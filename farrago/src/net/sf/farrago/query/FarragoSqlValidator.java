/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.query;

import java.math.*;

import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FarragoSqlValidator refines SqlValidator with some Farrago-specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoSqlValidator extends SqlValidator
{
    //~ Constructors ----------------------------------------------------------

    FarragoSqlValidator(FarragoPreparingStmt preparingStmt)
    {
        super(
            preparingStmt.getSqlOperatorTable(),
            preparingStmt,
            preparingStmt.getFarragoTypeFactory());
    }

    //~ Methods ---------------------------------------------------------------

    // override SqlValidator
    protected boolean shouldExpandIdentifiers()
    {
        // Farrago always wants to expand stars and identifiers during
        // validation since we use the validated representation as a canonical
        // form.
        return true;
    }

    // override SqlValidator
    protected boolean shouldAllowIntermediateOrderBy()
    {
        // Farrago follows the SQL standard on this.
        return false;
    }

    // override SqlValidator
    public void validateLiteral(SqlLiteral literal)
    {
        super.validateLiteral(literal);
        
        // REVIEW jvs 4-Aug-2005:  This should probably be calling over to the
        // available calculator implementations to see what they support.  For
        // now use ESP instead.
        switch (literal.getTypeName().getOrdinal()) {
        case SqlTypeName.Decimal_ordinal:
            BigDecimal bd = (BigDecimal) literal.getValue();
            if (bd.scale() == 0) {
                // Value must fit into a long.
                long longValue = bd.longValue();
                if (!BigDecimal.valueOf(longValue).equals(bd)) {
                    // overflow
                    throw newValidationError(
                        literal, EigenbaseResource.instance()
                        .newNumberLiteralOutOfRange(bd.toString()));
                }
            } else {
                // fall through for scaled case
            }

            // TODO jvs 4-Aug-2005:  support exact numerics,
            // which may also be able to handle overflow case above
            // if our maximum precision is bigger than that of a long
            validateLiteralAsDouble(literal);
            break;
        case SqlTypeName.Double_ordinal:
            validateLiteralAsDouble(literal);
            break;
        default:

            // no validation needed
            return;
        }
    }

    private void validateLiteralAsDouble(SqlLiteral literal)
    {
        BigDecimal bd = (BigDecimal) literal.getValue();
        double d = bd.doubleValue();
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            // overflow
            throw newValidationError(
                literal, EigenbaseResource.instance().newNumberLiteralOutOfRange(
                    Util.toScientificNotation(bd))
            );
        }

        // REVIEW jvs 4-Aug-2005:  what about underflow?
    }
}


// End FarragoSqlValidator.java
