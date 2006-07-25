/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.sql;

/**
 * Specification of a SQL sample.
 *
 * <p>For example, the query
 *
 * <blockquote>
 * <pre>SELECT *
 * FROM emp TABLESAMPLE SUBSTITUTE('medium')</pre>
 * </blockquote>
 *
 * declares a sample which is created using {@link #createNamed}.</p>
 *
 * <p>A sample is not a {@link SqlNode}. To include it in a parse tree, wrap it
 * as a literal, viz: {@link SqlLiteral#createSample(SqlSampleSpec,
 * SqlParserPos)}.
 */
public abstract class SqlSampleSpec
{

    //~ Constructors -----------------------------------------------------------

    protected SqlSampleSpec()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a sample which substitutes one relation for another.
     */
    public static SqlSampleSpec createNamed(String name)
    {
        return new SqlSubstitutionSampleSpec(name);
    }

    //~ Inner Classes ----------------------------------------------------------

    public static class SqlSubstitutionSampleSpec
        extends SqlSampleSpec
    {
        private final String name;

        private SqlSubstitutionSampleSpec(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }

        public String toString()
        {
            return
                "SUBSTITUTE("
                + SqlUtil.eigenbaseDialect.quoteStringLiteral(name)
                + ")";
        }
    }
}

// End SqlSampleSpec.java
