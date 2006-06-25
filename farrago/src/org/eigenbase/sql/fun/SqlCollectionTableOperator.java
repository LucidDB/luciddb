/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
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
package org.eigenbase.sql.fun;

import org.eigenbase.sql.SqlFunctionalOperator;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.SqlTypeStrategies;

/**
 * SqlCollectionTableOperator is the "table function derived table" operator. 
 * It converts a table-valued function into a relation,
 * e.g. "<code>SELECT * FROM TABLE(ramp(5))</code>".
 *
 * <p>This operator has function syntax (with one argument), whereas
 * {@link SqlStdOperatorTable#explicitTableOperator} is a prefix operator.
 * 
 * @author jhyde, stephan
 */
public class SqlCollectionTableOperator extends SqlFunctionalOperator
{
    public static final int MODALITY_RELATIONAL = 1;
    public static final int MODALITY_STREAM = 2;
    
    private final int modality;
    
    public SqlCollectionTableOperator(String name, int modality)
    {
        super(name, SqlKind.CollectionTable,
            200, true,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcAny);
        
        this.modality = modality;
    }
    
    public int getModality()
    {
        return modality;
    }
}
