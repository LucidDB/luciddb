/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.opt;

import java.util.*;
import net.sf.saffron.core.*;
import net.sf.saffron.rel.*;

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
    private Map accessMap;

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

    /**
     * Construct a TableAccessMap for all tables accessed by a SaffronRel and
     * its descendants.
     *
     * @param rel the SaffronRel for which to build the map
     */
    public TableAccessMap(SaffronRel rel)
    {
        accessMap = new HashMap();
        OptUtil.go(new TableRelVisitor(),rel);
    }

    /**
     * Determine whether a table is accessed at all.
     *
     * @param table the table of interest
     *
     * @return true if table is accessed
     */
    public boolean isTableAccessed(SaffronTable table)
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
    public boolean isTableAccessedForRead(SaffronTable table)
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
    public boolean isTableAccessedForWrite(SaffronTable table)
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
    public String getTableAccessMode(SaffronTable table)
    {
        String s = (String) accessMap.get(getKey(table));
        if (s == null) {
            return NO_ACCESS;
        }
        return s;
    }

    private Object getKey(SaffronTable table)
    {
        return Arrays.asList(table.getQualifiedName());
    }

    private class TableRelVisitor extends RelVisitor
    {
        // implement RelVisitor
        public void visit(SaffronRel p,int ordinal,SaffronRel parent)
        {
            super.visit(p,ordinal,parent);
            SaffronTable table = p.getTable();
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
            accessMap.put(key,newAccess);
        }
    }
}
