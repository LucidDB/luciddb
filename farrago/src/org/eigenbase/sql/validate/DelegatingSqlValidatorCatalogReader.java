/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
package org.eigenbase.sql.validate;

import java.util.List;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Implementation of
 * {@link org.eigenbase.sql.validate.SqlValidatorCatalogReader} that passes
 * all calls to a parent catalog reader.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 6, 2009
 */
public abstract class DelegatingSqlValidatorCatalogReader
    implements SqlValidatorCatalogReader
{
    protected final SqlValidatorCatalogReader catalogReader;

    /**
     * Creates a DelegatingSqlValidatorCatalogReader.
     *
     * @param catalogReader Parent catalog reader
     */
    public DelegatingSqlValidatorCatalogReader(
        SqlValidatorCatalogReader catalogReader)
    {
        this.catalogReader = catalogReader;
    }

    public SqlValidatorTable getTable(String[] names)
    {
        return catalogReader.getTable(names);
    }

    public RelDataType getNamedType(SqlIdentifier typeName)
    {
        return catalogReader.getNamedType(typeName);
    }

    public List<SqlMoniker> getAllSchemaObjectNames(List<String> names)
    {
        return catalogReader.getAllSchemaObjectNames(names);
    }

    public String getSchemaName()
    {
        return catalogReader.getSchemaName();
    }
}

// End DelegatingSqlValidatorCatalogReader.java
