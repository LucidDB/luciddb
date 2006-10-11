/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
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
package net.sf.farrago.runtime;

import java.sql.*;
import java.util.logging.*;

import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.session.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;

/**
 * FennelOnlyResultSet is a refinement of FarragoTupleIterResultSet, where
 * the result set consists of Fennel tuples.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelOnlyResultSet
    extends FarragoTupleIterResultSet
{
    //~ Static fields/initializers ---------------------------------------------
    
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelOnlyResultSet object.
     *
     * @param tupleIter underlying iterator
     * @param rowType type info for rows produced
     * @param runtimeContext runtime context for this execution
     * @param metaData metadata representing the result set
     */
    public FennelOnlyResultSet(
        TupleIter tupleIter,
        RelDataType rowType,
        FarragoSessionRuntimeContext runtimeContext,
        ResultSetMetaData metaData)
    {
        super(
            tupleIter,
            null,
            rowType,
            runtimeContext,
            new FennelColumnGetter(metaData, rowType));
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(toString());
        }
    }
    
    // implement AbstractResultSet
    protected Object getRaw(int columnIndex)
    {
        Object obj = super.getRaw(columnIndex);
        wasNull = (obj == null);
        return obj;
    }
    
    /**
     * ColumnGetter that reads columns from a Fennel tuple
     * 
     * @author Zelaine Fong
     */
    private static class FennelColumnGetter implements ColumnGetter
    {
        final private ResultSetMetaData metaData;
        final private RelDataType rowType;
        
        /**
         * @param metaData metadata corresponding to the result set from which
         * the columns will be read
         * @param rowType row type of the tuple containing the columns to be
         * read
         */
        public FennelColumnGetter(
            ResultSetMetaData metaData,
            RelDataType rowType)
        {
            this.metaData = metaData;
            this.rowType = rowType;
        }
        
        public String [] getColumnNames()
        {
            return RelOptUtil.getFieldNames(rowType);
        }

        public Object get(
            Object o,
            int columnIndex)
        {
            try {
                return FennelTupleResultSet.getRawColumnData(
                    columnIndex,
                    metaData,
                    (FennelTupleData) o);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }       
    }
}

// End FennelOnlyResultSet.java
