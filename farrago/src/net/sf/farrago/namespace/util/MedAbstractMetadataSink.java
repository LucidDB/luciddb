/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.namespace.util;

import java.io.*;

import java.util.*;
import java.util.regex.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.type.*;

import org.eigenbase.util.*;
import org.eigenbase.reltype.*;


/**
 * MedAbstractMetadataSink is an abstract base class for implementations of the
 * {@link FarragoMedMetadataSink} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractMetadataSink
    implements FarragoMedMetadataSink
{

    //~ Instance fields --------------------------------------------------------

    private final FarragoMedMetadataQuery query;
    private final FarragoTypeFactory typeFactory;
    private final Map patternMap;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new sink.
     *
     * @param query the query being processed; its filters will be implemented
     * by this sink
     * @param typeFactory factory for types written to this sink
     */
    protected MedAbstractMetadataSink(
        FarragoMedMetadataQuery query,
        FarragoTypeFactory typeFactory)
    {
        this.query = query;
        this.typeFactory = typeFactory;
        patternMap = new HashMap();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Tests whether an object should be included in the query result.
     *
     * @param objectName name of object
     * @param typeName name of object type
     * @param qualifier if true, test whether object is a valid qualifier; if
     * false, test whether object itself should be included
     *
     * @return true if the inclusion test passes
     */
    protected boolean shouldInclude(String objectName,
        String typeName,
        boolean qualifier)
    {
        if (!qualifier) {
            if (!query.getResultObjectTypes().contains(typeName)) {
                return false;
            }
        }
        FarragoMedMetadataFilter filter =
            (FarragoMedMetadataFilter) query.getFilterMap().get(typeName);
        if (filter == null) {
            return true;
        }
        boolean included = false;
        if (filter.getRoster() != null) {
            if (filter.getRoster().contains(objectName)) {
                included = true;
            }
        } else {
            Pattern pattern = getPattern(filter.getPattern());
            included = pattern.matcher(objectName).matches();
        }
        if (filter.isExclusion()) {
            included = !included;
        }
        return included;
    }

    // implement FarragoMedMetadataSink
    public FarragoTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    Pattern getPattern(String likePattern)
    {
        Pattern pattern = (Pattern) patternMap.get(likePattern);
        if (pattern == null) {
            // TODO jvs 6-Aug-2005:  move this to a common location
            // where it can be used for LIKE evaluations in SQL expressions,
            // and enhance it with escapes, etc.
            StringWriter regex = new StringWriter();
            int n = likePattern.length();
            for (int i = 0; i < n; ++i) {
                char c = likePattern.charAt(i);
                switch (c) {
                case '%':
                    regex.write(".*");
                    break;
                case '_':
                    regex.write(".");
                    break;
                default:
                    regex.write((int) c);
                    break;
                }
            }
            pattern = Pattern.compile(regex.toString());
            patternMap.put(likePattern, pattern);
        }
        return pattern;
    }

    // TODO jvs 28-Sept-2006:  Eliminate these backwards compatibility
    // adapters once all dependencies have been removed.

    public boolean writeObjectDescriptor(
        String name,
        String typeName,
        String remarks,
        Map map)
    {
        Properties properties = new Properties();
        properties.putAll(map);
        return
            writeObjectDescriptor(
                name,
                typeName,
                remarks,
                properties);
    }
    
    public boolean writeObjectDescriptor(
        String name,
        String typeName,
        String remarks,
        Properties properties)
    {
        return
            writeObjectDescriptor(
                name,
                typeName,
                remarks,
                (Map) properties);
    }
    
    public boolean writeColumnDescriptor(
        String tableName,
        String columnName,
        int ordinal,
        RelDataType type,
        String remarks,
        String defaultValue,
        Map map)
    {
        Properties properties = new Properties();
        properties.putAll(map);
        return
            writeColumnDescriptor(
                tableName,
                columnName,
                ordinal,
                type,
                remarks,
                defaultValue,
                properties);
    }
    
    public boolean writeColumnDescriptor(
        String tableName,
        String columnName,
        int ordinal,
        RelDataType type,
        String remarks,
        String defaultValue,
        Properties properties)
    {
        return
            writeColumnDescriptor(
                tableName,
                columnName,
                ordinal,
                type,
                remarks,
                defaultValue,
                (Map) properties);
    }
}

// End MedAbstractMetadataSink.java
