/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
 * SqlNullSemantics defines the possible comparison rules for values which might
 * be null. In SQL (and internal plans used to process SQL) different rules are
 * used depending on the context.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public enum SqlNullSemantics
{
    /**
     * Predicate semantics: e.g. in the expression (WHERE X=5), if X is NULL,
     * the comparison result is unknown, and so a filter used to evaluate the
     * WHERE clause rejects the row.
     */
    NULL_MATCHES_NOTHING,

    /**
     * GROUP BY key semantics: e.g. in the expression (GROUP BY A,B), the key
     * (null,5) is treated as equal to another key (null,5).
     */
    NULL_MATCHES_NULL,

    /**
     * Wildcard semantics: logically, this is not present in any SQL construct.
     * However, it is required internally, for example to rewrite NOT IN to NOT
     * EXISTS; when we negate a predicate, we invert the null semantics, so
     * NULL_MATCHES_NOTHING must become NULL_MATCHES_ANYTHING.
     */
    NULL_MATCHES_ANYTHING
}

// End SqlNullSemantics.java
