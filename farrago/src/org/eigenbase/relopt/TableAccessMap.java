/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * <code>TableAccessMap</code> represents the tables accessed by a query plan,
 * with READ/WRITE information.
 *
 * @author John V. Sichi
 * @version $Id$
 *
 */
public class TableAccessMap
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Table is accessed for read only.
     */
    public static final String READ_ACCESS = "R";

    /**
     * Table is accessed for write only.
     */
    public static final String WRITE_ACCESS = "W";

    /**
     * Table is accessed for both read and write.
     */
    public static final String READWRITE_ACCESS = "RW";

    /**
     * Table is not accessed at all.
     */
    public static final String NO_ACCESS = "N";

    //~ Instance fields -------------------------------------------------------

    private Map accessMap;

    //~ Constructors ----------------------------------------------------------

    /**
     * Construct a TableAccessMap for all tables accessed by a RelNode and
     * its descendants.
     *
     * @param rel the RelNode for which to build the map
     */
    public TableAccessMap(RelNode rel)
    {
        accessMap = new HashMap();
        RelOptUtil.go(
            new TableRelVisitor(),
            rel);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Determine whether a table is accessed at all.
     *
     * @param table the table of interest
     *
     * @return true if table is accessed
     */
    public boolean isTableAccessed(RelOptTable table)
    {
        return accessMap.containsKey(getKey(table));
    }

    /**
     * Determine whether a table is accessed for read.
     *
     * @param table the table of interest
     *
     * @return true if table is accessed for read
     */
    public boolean isTableAccessedForRead(RelOptTable table)
    {
        return getTableAccessMode(table).indexOf("R") > -1;
    }

    /**
     * Determine whether a table is accessed for write.
     *
     * @param table the table of interest
     *
     * @return true if table is accessed for write
     */
    public boolean isTableAccessedForWrite(RelOptTable table)
    {
        return getTableAccessMode(table).indexOf("W") > -1;
    }

    /**
     * Determine the access mode of a table.
     *
     * @param table the table of interest
     *
     * @return one of the _ACCESS constants
     */
    public String getTableAccessMode(RelOptTable table)
    {
        String s = (String) accessMap.get(getKey(table));
        if (s == null) {
            return NO_ACCESS;
        }
        return s;
    }

    private Object getKey(RelOptTable table)
    {
        return Arrays.asList(table.getQualifiedName());
    }

    //~ Inner Classes ---------------------------------------------------------

    private class TableRelVisitor extends RelVisitor
    {
        // implement RelVisitor
        public void visit(
            RelNode p,
            int ordinal,
            RelNode parent)
        {
            super.visit(p, ordinal, parent);
            RelOptTable table = p.getTable();
            if (table == null) {
                return;
            }
            String newAccess;
            if (p instanceof TableModificationRel) {
                newAccess = WRITE_ACCESS;
            } else {
                newAccess = READ_ACCESS;
            }
            Object key = getKey(table);
            String oldAccess = (String) accessMap.get(key);
            if ((oldAccess != null) && !oldAccess.equals(newAccess)) {
                newAccess = READWRITE_ACCESS;
            }
            accessMap.put(key, newAccess);
        }
    }
}
