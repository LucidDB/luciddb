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
package net.sf.saffron.sql;

import net.sf.saffron.resource.SaffronResource;
import net.sf.saffron.sql.parser.ParserUtil;
import net.sf.saffron.util.EnumeratedValues;
import net.sf.saffron.util.SaffronProperties;
import net.sf.saffron.util.Util;

import java.nio.charset.Charset;
import java.util.Locale;

/**
 * A <code>SqlCollation</code> is an object representing a Collate statement
 *
 * @author wael
 * @since Mar 23, 2004
 * @version $Id$
 **/
public class SqlCollation
{
    //~ Inner helper class ----------------------------------------------------
    /**
     * From SQL:99 ISO/IEC 9075-2:1999, section 4.2.3
     * <blockquote>
     * A &lt;character value expression&gt; consisting of a column reference has the coercibility characteristic
     * Implicit, with collating sequence as defined when the column was created. A &lt;character value
     * expression&gt; consisting of a value other than a column (e.g., a host variable or a literal) has the
     * coercibility characteristic Coercible, with the default collation for its character repertoire. A &lt;character
     * value expression&gt; simply containing a &lt;collate clause&gt; has the coercibility characteristic
     * Explicit, with the collating sequence specified in the &lt;collate clause&gt;.
     * </blockquote>
     */
    public static class Coercibility  extends EnumeratedValues.BasicValue {
        private Coercibility (int ordinal) {
            super("n/a".intern(),ordinal, null);
        }
        public static final int          Explicit_ordinal  = 0;  /* strongest */
        public static final Coercibility Explicit  =
                                            new Coercibility(Explicit_ordinal);
        public static final int          Implicit_ordinal = 1;
        public static final Coercibility Implicit =
                                            new Coercibility(Implicit_ordinal);
        public static final int          Coercible_ordinal = 2;
        public static final Coercibility Coercible =
                                            new Coercibility(Coercible_ordinal);
        public static final int          None_ordinal = 3;  /* weakest */
        public static final Coercibility None = new Coercibility(None_ordinal);
    }


    //~ Static fields/initializers --------------------------------------------

    //~ member variables ------------------------------------------------------
    protected String m_collationName;
    protected Charset m_charset;
    protected Locale m_locale;
    protected String m_strength = "";
    private Coercibility m_coercibility;

    /**
     * Creates a Collation by its name and its coercibility
     * @param collation
     * @param coercibility
     */
    public SqlCollation(String collation, Coercibility coercibility) {
        m_coercibility = coercibility;
        Object[] parseValues = ParserUtil.parseCollation(collation);
        m_charset = (Charset) parseValues[0];
        m_locale =  (Locale)  parseValues[1];
        m_strength= (String)  parseValues[2];
        m_collationName =m_charset.name().toUpperCase()+"$"+m_locale.toString();
        if (m_strength!=null && m_strength.length()>0) {
            m_collationName += "$"+m_strength;
        }
    }

    public boolean equals(Object o) {
        return (o instanceof SqlCollation) &&
               ((SqlCollation) o).getCollationName().equals(
                       this.getCollationName());
    }

    /**
     * Creates a SqlCollation with the default collation name and the given coercibility
     * @param coercibility
     */
    public SqlCollation(Coercibility coercibility) {
        this(SaffronProperties.instance().defaultCollation.get(),
                coercibility);
    }

    //~ member functions -------------------------------------------------------------

    /**
     * Returns the collating sequence (the collation name) and the coercibility the resulting value shall have per
     * SQL:99 ISO/IEC 9075-2:1999, section 4.2.3, table 2
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. The "no collating sequence" result is returned as null.
     */
    public static SqlCollation getCoercibilityDyadicOperator(SqlCollation col1, SqlCollation col2) {
        return getCoercibilityDyadic(col1, col2);
    }

    /**
     * Returns the collating sequence (the collation name) to use for a comparison per
     * SQL:99 ISO/IEC 9075-2:1999, section 4.2.3, table 3
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. The "no collating sequence" result is returned as null.
     */
    public static String getCoercibilityDyadicComparison(SqlCollation col1, SqlCollation col2) {
        SqlCollation ret = getCoercibilityDyadic(col1, col2);
        if (null==ret) {
            //todo change exception type
            //todo improve error msg
            throw SaffronResource.instance().newValidationError(
                   "Invalid compare. Comparing  (collation, coercibility): "+
                   "("+col1.m_collationName+", "+col1.m_coercibility+") with "+
                   "("+col2.m_collationName+", "+col2.m_coercibility+") is illegal");
            }

        return ret.m_collationName;
    }

    /**
     * Returns the result for {@link #getCoercibilityDyadicComparison} and {@link #getCoercibilityDyadicOperator}
     */
    protected static SqlCollation getCoercibilityDyadic(SqlCollation col1, SqlCollation col2) {
        assert(null!=col1);
        assert(null!=col2);
        if (col1.getCoercibility().equals(Coercibility.Coercible)) {
            switch (col2.getCoercibility().getOrdinal()) {
            case Coercibility.Coercible_ordinal: return new SqlCollation(col2.m_collationName, Coercibility.Coercible);
            case Coercibility.Implicit_ordinal: return new SqlCollation(col2.m_collationName, Coercibility.Implicit);
            case Coercibility.None_ordinal: return null;
            case Coercibility.Explicit_ordinal: return new SqlCollation(col2.m_collationName, Coercibility.Explicit);
            default: throw new AssertionError("Should never come here");

            }
        }

        if (col1.getCoercibility().equals(Coercibility.Implicit)) {
            switch (col2.getCoercibility().getOrdinal()) {
            case Coercibility.Coercible_ordinal: return new SqlCollation(col1.m_collationName, Coercibility.Implicit);
            case Coercibility.Implicit_ordinal:
                if(col1.m_collationName.equals(col2.m_collationName)) {
                    return new SqlCollation(col2.m_collationName, Coercibility.Implicit);
                }
                return null;
            case Coercibility.None_ordinal: return null;
            case Coercibility.Explicit_ordinal: return new SqlCollation(col2.m_collationName, Coercibility.Explicit);
            default:  throw new AssertionError("Should never come here");
            }
        }

        if (col1.getCoercibility().equals(Coercibility.None)) {
            switch (col2.getCoercibility().getOrdinal()) {
            case Coercibility.Coercible_ordinal:
            case Coercibility.Implicit_ordinal:
            case Coercibility.None_ordinal: return null;
            case Coercibility.Explicit_ordinal:
                    return new SqlCollation(col2.m_collationName,
                                            Coercibility.Explicit);
            default:  throw new AssertionError("Should never come here");

            }
        }

        if (col1.getCoercibility().equals(Coercibility.Explicit)) {
            switch (col2.getCoercibility().getOrdinal()) {
            case Coercibility.Coercible_ordinal:
            case Coercibility.Implicit_ordinal:
            case Coercibility.None_ordinal: return new SqlCollation(col1.m_collationName, Coercibility.Explicit);
            case Coercibility.Explicit_ordinal:
                if(col1.m_collationName.equals(col2.m_collationName)) {
                    return new SqlCollation(col2.m_collationName, Coercibility.Explicit);
                }
                throw SaffronResource.instance().newValidationError(
                       "Invalid syntax. Two explicit different collations ("+
                       col1.m_collationName+", "+col2.m_collationName+") are illegal");
            }
        }

        throw Util.newInternal("Should never come here");
    }

    public Object clone() {
        return new SqlCollation(m_collationName, m_coercibility);
    }

    public String toString() {
        return "COLLATE "+m_collationName;
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(this.toString());
    }

    public Charset getCharset() {
        return m_charset;
    }

    public final String getCollationName() {
        return m_collationName;
    }

    public final SqlCollation.Coercibility getCoercibility() {
        return m_coercibility;
    }
}


// End SqlCase.java
