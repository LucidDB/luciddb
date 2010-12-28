/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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

    /**
     * Creates a table sample without repeatability.
     *
     * @param isBernoulli true if Bernoulli style sampling is to be used; false
     * for implementation specific sampling
     * @param samplePercentage likelihood of a row appearing in the sample
     */
    public static SqlSampleSpec createTableSample(
        boolean isBernoulli,
        float samplePercentage)
    {
        return new SqlTableSampleSpec(isBernoulli, samplePercentage);
    }

    /**
     * Creates a table sample with repeatability.
     *
     * @param isBernoulli true if Bernoulli style sampling is to be used; false
     * for implementation specific sampling
     * @param samplePercentage likelihood of a row appearing in the sample
     * @param repeatableSeed seed value used to reproduce the same sample
     */
    public static SqlSampleSpec createTableSample(
        boolean isBernoulli,
        float samplePercentage,
        int repeatableSeed)
    {
        return new SqlTableSampleSpec(
            isBernoulli,
            samplePercentage,
            repeatableSeed);
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
            return "SUBSTITUTE("
                + SqlDialect.EIGENBASE.quoteStringLiteral(name)
                + ")";
        }
    }

    public static class SqlTableSampleSpec
        extends SqlSampleSpec
    {
        private final boolean isBernoulli;
        private final float samplePercentage;
        private final boolean isRepeatable;
        private final int repeatableSeed;

        private SqlTableSampleSpec(boolean isBernoulli, float samplePercentage)
        {
            this.isBernoulli = isBernoulli;
            this.samplePercentage = samplePercentage;
            this.isRepeatable = false;
            this.repeatableSeed = 0;
        }

        private SqlTableSampleSpec(
            boolean isBernoulli,
            float samplePercentage,
            int repeatableSeed)
        {
            this.isBernoulli = isBernoulli;
            this.samplePercentage = samplePercentage;
            this.isRepeatable = true;
            this.repeatableSeed = repeatableSeed;
        }

        /**
         * Indicates Bernoulli vs. System sampling.
         */
        public boolean isBernoulli()
        {
            return isBernoulli;
        }

        /**
         * Returns sampling percentage. Range is 0.0 to 1.0, exclusive
         */
        public float getSamplePercentage()
        {
            return samplePercentage;
        }

        /**
         * Indicates whether repeatable seed should be used.
         */
        public boolean isRepeatable()
        {
            return isRepeatable;
        }

        /**
         * Seed to produce repeatable samples.
         */
        public int getRepeatableSeed()
        {
            return repeatableSeed;
        }

        public String toString()
        {
            StringBuilder b = new StringBuilder();
            b.append(isBernoulli ? "BERNOULLI" : "SYSTEM");
            b.append('(');
            b.append(samplePercentage * 100.0);
            b.append(')');

            if (isRepeatable) {
                b.append(" REPEATABLE(");
                b.append(repeatableSeed);
                b.append(')');
            }
            return b.toString();
        }
    }
}

// End SqlSampleSpec.java
