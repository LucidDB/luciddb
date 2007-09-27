/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
// Portions Copyright (C) 2007-2007 John V. Sichi
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
package org.eigenbase.relopt;

/**
 * RelOptSamplingParameters represents the parameters necessary to produce
 * a sample of a relation.
 * 
 * <p>It's parameters are derived from the SQL 2003 TABLESAMPLE clause.
 * 
 * @author Stephan Zuercher
 */
public class RelOptSamplingParameters
{
    private final boolean isBernoulli;
    private final float samplingPercentage;
    private final boolean isRepeatable;
    private final int repeatableSeed;
    
    public RelOptSamplingParameters(
        boolean isBernoulli,
        float samplingPercentage,
        boolean isRepeatable,
        int repeatableSeed)
    {
        this.isBernoulli = isBernoulli;
        this.samplingPercentage = samplingPercentage;
        this.isRepeatable = isRepeatable;
        this.repeatableSeed = repeatableSeed;
    }

    /**
     * Indicates whether Bernoulli or system sampling should be performed.
     * Bernoulli sampling requires the decision whether to include each row in 
     * the the sample to be independent across rows.  System sampling allows
     * implementation-dependent behavior.
     * 
     * 
     * @return true if Bernoulli sampling is configured, false for system 
     *         sampling
     */
    public boolean isBernoulli()
    {
        return isBernoulli;
    }
    
    /**
     * Returns the sampling percentage.  For Bernoulli sampling, the sampling 
     * percentage is the likelihood that any given row will be included in
     * the sample.  For system sampling, the sampling percentage indicates
     * (roughly) what percentage of the rows will appear in the sample.   
     * 
     * @return the sampling percentage between 0.0 and 1.0, exclusive
     */
    public float getSamplingPercentage()
    {
        return samplingPercentage;
    }
    
    /**
     * Indicates whether the sample results should be repeatable.  Sample
     * results are only required to repeat if no changes have been made
     * to the relation's content or structure.  If the sample is configured
     * to be repeatable, then a user-specified seed value can be obtained via
     * {@link #getRepeatableSeed()}.
     * 
     * @return true if the sample results should be repeatable
     */
    public boolean isRepeatable()
    {
        return isRepeatable;
    }
    
    /**
     * If {@link #isRepeatable()} returns <tt>true</tt>, this method returns
     * a user-specified seed value.  Samples of the same, unmodified relation
     * should be identical if the sampling mode, sampling percentage and 
     * repeatable seed are the same.
     * 
     * @return seed value for repeatable samples
     */
    public int getRepeatableSeed()
    {
        return repeatableSeed;
    }
}
