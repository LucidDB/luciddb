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

package net.sf.farrago.query;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;

import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.core.*;

import openjava.ptree.*;


/**
 * FennelRel defines the interface which must be implemented by any SaffronRel
 * corresponding to a C++ physical implementation conforming to
 * the fennel::ExecutionStream interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FennelRel
{
    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return the connection expression corresponding to getPreparingStmt()
     */
    public SaffronConnection getConnection();

    /**
     * .
     *
     * @return the FarragoPreparingStmt producing this relation
     */
    public FarragoPreparingStmt getPreparingStmt();

    /**
     * Convert this relational expression to FemExecutionStreamDef form.
     *
     * @param implementor for generating Java code
     *
     * @return generated FemExecutionStreamDef
     */
    public FemExecutionStreamDef toStreamDef(
        FarragoRelImplementor implementor);

    /**
     * .
     *
     * @return the sort order produced by this FennelRel, or an empty array if
     * the output is not guaranteed to be in any particular order
     */
    public RelFieldCollation [] getCollations();
}


// End FennelRel.java
