/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
package net.sf.farrago.namespace;

import java.util.*;


/**
 * FarragoMedMetadataQuery defines a metadata query processed by {@link
 * FarragoMedNameDirectory}. For some namespaces, it may be more efficient to
 * query different kinds of metadata simultaneously; for example, a flat-file
 * reader might return all files in a directory as tables, with the header of
 * each file providing a column list. For this reason, the interface allows more
 * than one object type to be queried together.
 *
 * <p>Object types are identified using CWM class names. The following types are
 * currently defined (see members starting with prefix "OTN_"):
 *
 * <ul>
 * <li><em>Table</em>: any table-like object such as a base table or view
 * <li><em>Column</em>: a column
 * <li><em>Schema</em>: a schema (acts as a subdirectory)
 * <li><em>Package</em>: a logical package (acts as a subdirectory)
 * </ul>
 *
 * <p>Results from metadata queries are written via the {@link
 * FarragoMedMetadataSink} interface. Only immediate contents of the queried
 * directory should be returned; some results may be subdirectories which can be
 * queried independently.
 *
 * <p>Note that implementation of query filtering and projection is always
 * optional. An unsophisticated implementation is free to produce results which
 * do not match all restrictions, so consumers are responsible for reapplying
 * them to each object produced.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedMetadataQuery
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String OTN_TABLE = "Table";

    public static final String OTN_COLUMN = "Column";

    public static final String OTN_SCHEMA = "Schema";

    public static final String OTN_PACKAGE = "Package";

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves a map of filters to be applied to queried metadata. Map keys
     * are object type names, and values are instances of the {@link
     * FarragoMedMetadataFilter} interface. The conjunction of all relevant
     * filters is used. Metadata queries take place in the context of an
     * instance of {@link FarragoMedNameDirectory}, which provides additional
     * filtering context.
     *
     * @return map
     */
    public Map<String, FarragoMedMetadataFilter> getFilterMap();

    /**
     * Retrieves a set of result object types to be returned by the query,
     * identified by object type name.
     *
     * @return Set<String>
     */
    public Set<String> getResultObjectTypes();
}

// End FarragoMedMetadataQuery.java
