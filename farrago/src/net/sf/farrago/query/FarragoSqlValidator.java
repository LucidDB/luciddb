/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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
package net.sf.farrago.query;

import java.math.*;
import java.util.*;

import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;
import org.eigenbase.reltype.RelDataTypeFactory;


/**
 * FarragoSqlValidator refines SqlValidator with some Farrago-specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSqlValidator
    extends SqlValidatorImpl
{

    final FarragoPreparingStmt preparingStmt;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructor that allows caller to specify dependant objects rather
     * than relying on the preparingStmt to supply them.  This constructor is
     * is friendlier to class extension as well as providing more control during
     * test setup.
     */
    public FarragoSqlValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        Compatible compatible,
        FarragoPreparingStmt preparingStmt)
    {
        super(
            opTab,
            catalogReader,
            typeFactory,
            compatible);

        this.preparingStmt = preparingStmt;
    }

    /**
     * Constructor that relies on the preparingStmt object to provide various
     * other objects during initialization.
     */
    public FarragoSqlValidator(
        FarragoPreparingStmt preparingStmt,
        Compatible compatible)
    {
        super(
            preparingStmt.getSqlOperatorTable(),
            preparingStmt,
            preparingStmt.getFarragoTypeFactory(),
            compatible);

        this.preparingStmt = preparingStmt;
    }

    //~ Methods ----------------------------------------------------------------

    // override SqlValidator
    public SqlNode validate(SqlNode topNode)
    {
        SqlNode node = super.validate(topNode);
        getPreparingStmt().analyzeRoutineDependencies(node);
        return node;
    }

    // override SqlValidator
    public boolean shouldExpandIdentifiers()
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

    public void validateDataType(SqlDataTypeSpec dataType)
    {
        super.validateDataType(dataType);
        FarragoPreparingStmt preparingStmt = getPreparingStmt();
        try {
            preparingStmt.getStmtValidator().validateDataType(dataType);
        } catch (SqlValidatorException ex) {
            throw newValidationError(dataType, ex);
        }
    }

    protected FarragoPreparingStmt getPreparingStmt()
    {
        return preparingStmt;
    }

    // override SqlValidatorImpl
    public void validateInsert(SqlInsert call)
    {
        getPreparingStmt().setDmlValidation(
            call.getTargetTable(),
            PrivilegedActionEnum.INSERT);
        super.validateInsert(call);
        getPreparingStmt().clearDmlValidation();
    }

    // override SqlValidatorImpl
    public void validateUpdate(SqlUpdate call)
    {
        getPreparingStmt().setDmlValidation(
            call.getTargetTable(),
            PrivilegedActionEnum.UPDATE);
        super.validateUpdate(call);
        getPreparingStmt().clearDmlValidation();
    }

    // override SqlValidatorImpl
    public void validateDelete(SqlDelete call)
    {
        getPreparingStmt().setDmlValidation(
            call.getTargetTable(),
            PrivilegedActionEnum.DELETE);
        super.validateDelete(call);
        getPreparingStmt().clearDmlValidation();
    }

    // override SqlValidatorImpl
    public void validateMerge(SqlMerge call)
    {
        getPreparingStmt().setDmlValidation(
            call.getTargetTable(),
            PrivilegedActionEnum.UPDATE);
        super.validateMerge(call);
        getPreparingStmt().clearDmlValidation();
    }

    // override SqlValidatorImpl
    protected void validateFeature(
        ResourceDefinition feature,
        SqlParserPos context)
    {
        super.validateFeature(feature, context);
        getPreparingStmt().getStmtValidator().validateFeature(
            feature,
            context);
    }

    // override SqlValidatorImpl
    public void validateColumnListParams(
        SqlFunction function,
        RelDataType [] argTypes,
        SqlNode [] operands)
    {
        // get the UDR that the function corresponds to
        FarragoUserDefinedRoutine routine =
            (FarragoUserDefinedRoutine) function;
        FemRoutine femRoutine = routine.getFemRoutine();
        List<CwmParameter> params = femRoutine.getParameter();
        Map<Integer, SqlSelect> cursorMap = cursorMapStack.peek();

        // locate arguments that are COLUMN_LIST types; locate the select
        // scope corresponding to the source cursor and revalidate the
        // function operand using that scope
        for (int i = 0; i < argTypes.length; i++) {
            if (argTypes[i].getSqlTypeName() == SqlTypeName.COLUMN_LIST) {
                FemColumnListRoutineParameter clParam =
                    (FemColumnListRoutineParameter) params.get(i);
                String sourceCursor = clParam.getSourceCursorName();
                int cursorPosition = -1;
                for (FemRoutineParameter p :
                    Util.cast(params, FemRoutineParameter.class))
                {
                    if (p.getType().getName().equals("CURSOR")) {
                        cursorPosition++;
                        if (p.getName().equals(sourceCursor)) {
                            SqlSelect sourceSelect =
                                cursorMap.get(cursorPosition);
                            SqlValidatorScope cursorScope =
                                getCursorScope(sourceSelect);
                            // save the original node type so we can reset it
                            // after we've validated the column references
                            RelDataType origNodeType =
                                getValidatedNodeType(operands[i]);
                            removeValidatedNodeType(operands[i]);
                            deriveType(cursorScope, operands[i]);
                            setValidatedNodeType(operands[i], origNodeType);
                            break;
                        }
                    }
                }
            }
        }
    }
}

// End FarragoSqlValidator.java
