/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import java.nio.charset.Charset;
import java.util.Locale;

import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;


/**
 * A <code>SqlCollation</code> is an object representing a Collate statement
 *
 * @author wael
 * @since Mar 23, 2004
 * @version $Id$
 **/
public class SqlCollation
{
    //~ Instance fields -------------------------------------------------------

    protected final String collationName;
    protected final Charset charset;
    protected final Locale locale;
    protected final String strength;
    private final Coercibility coercibility;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a Collation by its name and its coercibility
     * @param collation
     * @param coercibility
     */
    public SqlCollation(
        String collation,
        Coercibility coercibility)
    {
        this.coercibility = coercibility;
        SqlParserUtil.ParsedCollation parseValues =
            SqlParserUtil.parseCollation(collation);
        charset = parseValues.charset;
        locale = parseValues.locale;
        strength = parseValues.strength;
        String c = charset.name().toUpperCase() + "$" + locale.toString();
        if ((strength != null) && (strength.length() > 0)) {
            c += ("$" + strength);
        }
        collationName = c;
    }

    /**
     * Creates a SqlCollation with the default collation name and the given
     * coercibility.
     * 
     * @param coercibility
     */
    public SqlCollation(Coercibility coercibility)
    {
        this(
            SaffronProperties.instance().defaultCollation.get(),
            coercibility);
    }

    //~ Methods ---------------------------------------------------------------

    public boolean equals(Object o)
    {
        return (o instanceof SqlCollation)
            && ((SqlCollation) o).getCollationName().equals(
                this.getCollationName());
    }

    /**
     * Returns the collating sequence (the collation name) and the coercibility
     * for the resulting value of a dyadic operator.
     *
     * @sql.99 Part 2 Section 4.2.3 Table 2
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. The "no collating sequence" result is returned as null.
     */
    public static SqlCollation getCoercibilityDyadicOperator(
        SqlCollation col1,
        SqlCollation col2)
    {
        return getCoercibilityDyadic(col1, col2);
    }

    /**
     * Returns the collating sequence (the collation name) and the coercibility
     * for the resulting value of a dyadic operator.
     *
     * @sql.99 Part 2 Section 4.2.3 Table 2
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. If no collating sequence could
     * be deduced a {@link EigenbaseResource#newInvalidCompare} is thrown
     */
    public static SqlCollation getCoercibilityDyadicOperatorThrows(
        SqlCollation col1,
        SqlCollation col2)
    {
        SqlCollation ret = getCoercibilityDyadic(col1, col2);
        if (null == ret) {
            throw EigenbaseResource.instance().newInvalidCompare(col1.collationName,
                "" + col1.coercibility, col2.collationName,
                "" + col2.coercibility);
        }
        return ret;
    }

    /**
     * Returns the collating sequence (the collation name) to use for
     * the resulting value of a comparison.
     *
     * @sql.99 Part 2 Section 4.2.3 Table 3
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. If no collating sequence could
     * be deduced a {@link EigenbaseResource#newInvalidCompare} is thrown
     */
    public static String getCoercibilityDyadicComparison(
        SqlCollation col1,
        SqlCollation col2)
    {
        return getCoercibilityDyadicOperatorThrows(col1, col2).collationName;
    }

    /**
     * Returns the result for {@link #getCoercibilityDyadicComparison} and {@link #getCoercibilityDyadicOperator}
     */
    protected static SqlCollation getCoercibilityDyadic(
        SqlCollation col1,
        SqlCollation col2)
    {
        assert (null != col1);
        assert (null != col2);
        if (col1.getCoercibility().equals(Coercibility.Coercible)) {
            switch (col2.getCoercibility().getOrdinal()) {
            case Coercibility.Coercible_ordinal:
                return new SqlCollation(col2.collationName,
                    Coercibility.Coercible);
            case Coercibility.Implicit_ordinal:
                return new SqlCollation(col2.collationName,
                    Coercibility.Implicit);
            case Coercibility.None_ordinal:
                return null;
            case Coercibility.Explicit_ordinal:
                return new SqlCollation(col2.collationName,
                    Coercibility.Explicit);
            default:
                throw new AssertionError("Should never come here");
            }
        }

        if (col1.getCoercibility().equals(Coercibility.Implicit)) {
            switch (col2.getCoercibility().getOrdinal()) {
            case Coercibility.Coercible_ordinal:
                return new SqlCollation(col1.collationName,
                    Coercibility.Implicit);
            case Coercibility.Implicit_ordinal:
                if (col1.collationName.equals(col2.collationName)) {
                    return new SqlCollation(col2.collationName,
                        Coercibility.Implicit);
                }
                return null;
            case Coercibility.None_ordinal:
                return null;
            case Coercibility.Explicit_ordinal:
                return new SqlCollation(col2.collationName,
                    Coercibility.Explicit);
            default:
                throw new AssertionError("Should never come here");
            }
        }

        if (col1.getCoercibility().equals(Coercibility.None)) {
            switch (col2.getCoercibility().getOrdinal()) {
            case Coercibility.Coercible_ordinal:
            case Coercibility.Implicit_ordinal:
            case Coercibility.None_ordinal:
                return null;
            case Coercibility.Explicit_ordinal:
                return new SqlCollation(col2.collationName,
                    Coercibility.Explicit);
            default:
                throw new AssertionError("Should never come here");
            }
        }

        if (col1.getCoercibility().equals(Coercibility.Explicit)) {
            switch (col2.getCoercibility().getOrdinal()) {
            case Coercibility.Coercible_ordinal:
            case Coercibility.Implicit_ordinal:
            case Coercibility.None_ordinal:
                return new SqlCollation(col1.collationName,
                    Coercibility.Explicit);
            case Coercibility.Explicit_ordinal:
                if (col1.collationName.equals(col2.collationName)) {
                    return new SqlCollation(col2.collationName,
                        Coercibility.Explicit);
                }
                throw EigenbaseResource.instance().newDifferentCollations(col1.collationName,
                    col2.collationName);
            }
        }

        throw Util.newInternal("Should never come here");
    }

    public String toString()
    {
        return "COLLATE " + collationName;
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.print(this.toString());
    }

    public Charset getCharset()
    {
        return charset;
    }

    public final String getCollationName()
    {
        return collationName;
    }

    public final SqlCollation.Coercibility getCoercibility()
    {
        return coercibility;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * <blockquote>
     * A &lt;character value expression&gt; consisting of a column reference has the coercibility characteristic
     * Implicit, with collating sequence as defined when the column was created. A &lt;character value
     * expression&gt; consisting of a value other than a column (e.g., a host variable or a literal) has the
     * coercibility characteristic Coercible, with the default collation for its character repertoire. A &lt;character
     * value expression&gt; simply containing a &lt;collate clause&gt; has the coercibility characteristic
     * Explicit, with the collating sequence specified in the &lt;collate clause&gt;.
     * </blockquote>
     *
     * @sql.99 Part 2 Section 4.2.3
     */
    public static class Coercibility extends EnumeratedValues.BasicValue
    {
        public static final int Explicit_ordinal = 0; /* strongest */
        public static final Coercibility Explicit =
            new Coercibility(Explicit_ordinal);
        public static final int Implicit_ordinal = 1;
        public static final Coercibility Implicit =
            new Coercibility(Implicit_ordinal);
        public static final int Coercible_ordinal = 2;
        public static final Coercibility Coercible =
            new Coercibility(Coercible_ordinal);
        public static final int None_ordinal = 3; /* weakest */
        public static final Coercibility None = new Coercibility(None_ordinal);

        private Coercibility(int ordinal)
        {
            super(
                "n/a".intern(),
                ordinal,
                null);
        }
    }
}


// End SqlCase.java
