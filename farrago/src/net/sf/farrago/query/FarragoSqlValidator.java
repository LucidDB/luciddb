/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

package net.sf.farrago.query;

import net.sf.saffron.sql.*;

import net.sf.farrago.resource.*;

/**
 * FarragoSqlValidator refines SqlValidator with some Farrago-specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoSqlValidator extends SqlValidator
{
    FarragoSqlValidator(FarragoPreparingStmt preparingStmt)
    {
        super(preparingStmt.getSqlOperatorTable(),
                preparingStmt,
                preparingStmt.getFarragoTypeFactory());
    }

    // override SqlValidator
    public RuntimeException newValidationError(String s)
    {
        // TODO:  need to integrate i18n with Saffron
        return FarragoResource.instance().newValidatorUntranslated(s);
    }

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
}

// End FarragoSqlValidator.java
