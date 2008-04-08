/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.*;

import net.sf.farrago.session.*;

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql2rel.*;


/**
 * ScalarSubqueryConverter converts subqueries to scalar constants by
 * evaulating them and passing back the resulting constant expression.
 * By doing so, this means that the statement containing the subquery can no
 * longer be cached.
 * 
 * <p> 
 * This class can also be used to convert EXISTS subqueries by replacing
 * the EXISTS with a boolean value indicating whether the subquery returns
 * zero (FALSE) or at least one (TRUE) row.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class ScalarSubqueryConverter
    implements SubqueryConverter
{
    //~ Instance fields
    
    private final FarragoSessionPreparingStmt stmt;
    
    //~ Constructors -----------------------------------------------------------
    
    public ScalarSubqueryConverter(FarragoSessionPreparingStmt stmt)
    {
        this.stmt = stmt;
    }
    
    //~ Methods ----------------------------------------------------------------
    
    // implement SubqueryConverter
    public boolean canConvertSubquery()
    {
        return true;
    }

    // implement SubqueryConverter
    public RexNode convertSubquery(
        SqlCall subquery,
        SqlToRelConverter parentConverter,
        boolean isExists,
        boolean isExplain)
    {
        // Use a FarragoReentrantSubquery to evaluate the subquery
        List<RexNode> reducedValues = new ArrayList<RexNode>();
        FarragoReentrantSubquery reentrantStmt =
            new FarragoReentrantSubquery(
                subquery,
                parentConverter,
                isExists,
                isExplain,
                reducedValues);
        reentrantStmt.execute(stmt.getSession(), true);
        if (reentrantStmt.failed) {
            return null;
        } else {
            stmt.disableStatementCaching();
            return reducedValues.get(0);
        }
    }
}

// End ScalarSubqueryConverter.java
