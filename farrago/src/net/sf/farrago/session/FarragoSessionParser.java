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

package net.sf.farrago.session;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;

/**
 * FarragoSessionParser represents an object capable of parsing Farrago
 * SQL statements.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionParser
{
    /**
     * Parses an SQL statement.  If it is a DDL statement, this may implicitly
     * performs uncommitted catalog updates.
     *
     * @param ddlValidator the validator to use for lookup during parsing
     * if this turns out to be a DDL statement
     *
     * @param txnContext transaction context to use for initiating
     * repository transactions
     *
     * @param sql the SQL text to be parsed
     *
     * @return for DDL, a FarragoSessionDdlStmt; for DML or query, top-level
     * SqlNode
     */
    public Object parseSqlStatement(
        FarragoSessionDdlValidator ddlValidator,
        FarragoReposTxnContext txnContext,
        String sql);

    /**
     * @return the current position, or null if done parsing
     */
    public FarragoSessionParserPosition getCurrentPosition();
}

// End FarragoSessionParser.java
