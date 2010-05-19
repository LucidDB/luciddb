/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007 The Eigenbase Project
// Copyright (C) 2007 SQLstream, Inc.
// Copyright (C) 2007 Dynamo BI Corporation
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
package net.sf.farrago.util;

import java.util.*;


/**
 * FarragoCardinalityEstimator estimates the number of distinct values in a
 * population based on a sample of that population. The algorithms in this class
 * are adapted from "Estimating the Number of Classes in a Finite Population" by
 * Peter J. Haas and Lynne Stokes (IBM Research Journal; RJ 10025; May 1996).
 *
 * @author Stephan Zuercher
 */
public class FarragoCardinalityEstimator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int Duj2a_DIVIDER = 50;

    //~ Instance fields --------------------------------------------------------

    /**
     * Controls whether {@link #f}, {@link #f_small}, {@link #B}, {@link
     * #n_small}, and {@link #dn_small} are computed. Alters behavior of {@link
     * #estimate()}.
     */
    private final boolean assumeUniqueConstraint;

    /**
     * The full population's size.
     */
    private final long N;

    /**
     * The number of items in the sample.
     */
    private long n;

    /**
     * The number of classes (distinct values) in the sample.
     */
    private long dn;

    /**
     * Sparse array of class frequencies. f[i] is the number of classes
     * (distinct values) that appear exactly k times in the sample.
     */
    private SparseLongArray f;

    /**
     * For computation of Duj2a, the number of items in the sample that appear
     * fewer than {@link #Duj2a_DIVIDER} times. Falls in the range [0, n].
     */
    private long n_small;

    /**
     * For computation of Duj2a, the number of classes (distinct values) that
     * appear fewer than {@link #Duj2a_DIVIDER} times. Falls in the range [0,
     * dn].
     */
    private long dn_small;

    /**
     * For computation of Duj2a, a sparse array of class frequencies. The
     * maximum index in this array is {@link #Duj2a_DIVIDER} - 1. May contain no
     * entries.
     */
    private SparseLongArray f_small;

    /**
     * The number of items in each class where the class appears greater than
     * {@link #Duj2a_DIVIDER} times. May be empty or have as many entries as
     * calls to {@link #addSampleClass(long, boolean)} (if all classes appear
     * more than Duj2a_DIVIDER times.
     */
    private List<Long> B;

    /**
     * Number of null values in the sample.
     */
    private long nullClassSize;

    //~ Constructors -----------------------------------------------------------

    /**
     * Construct a FarragoCardinalityEstimator.
     *
     * <p>If the <tt>assumeUniqueConstraint</tt> flag is set, some intermediate
     * values are not computed. The {@link #estimate()} method will simply
     * return either the (given) population size or the result of {@link
     * #estimateDistinctWithNullClass()} if a null class was seen. The methods
     * {@link #estimateDistinctWithNullClass()}, {@link #getSampleSize()}, and
     * {@link #getNumSampleClasses()} continue to work as normal.
     *
     * @param populationSize the size of the full population
     * @param assumeUniqueConstraint if true, some intermediate values are not
     * calculated (see above)
     */
    public FarragoCardinalityEstimator(
        long populationSize,
        boolean assumeUniqueConstraint)
    {
        this.N = populationSize;
        this.assumeUniqueConstraint = assumeUniqueConstraint;

        if (!assumeUniqueConstraint) {
            this.f = new SparseLongArray();
            this.f_small = new SparseLongArray();
            this.B = new ArrayList<Long>();
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Add a pre-aggregated sample of the population to the estimator.
     * Pre-aggregated sample means that this method is called exactly once for
     * each class (distinct value) in the sample, with the number of times the
     * class apears in sample given as <tt>classSize</tt>. Classes need not be
     * added in any particular order. The goal of this method is to compute the
     * data structures necessary to estimate cardinality of the sample without
     * requiring multiple passes through the sample itself.
     *
     * @param classSize the number of times the class appears
     * @param isNullClass set true if this class has no value (may only be done
     * once per sample)
     *
     * @throws IllegalArgumentException if classSize is zero or negative, or if
     * multiple null classes are specified.
     */
    public void addSampleClass(long classSize, boolean isNullClass)
    {
        if (classSize <= 0) {
            throw new IllegalArgumentException(
                "classSize " + classSize + " is invalid");
        }

        n += classSize;
        dn++;

        if (isNullClass) {
            if (nullClassSize != 0) {
                throw new IllegalArgumentException(
                    "Multiple null classes are not allowed");
            }

            nullClassSize = classSize;
        }

        if (assumeUniqueConstraint) {
            return;
        }

        f.increment(classSize);

        // REVIEW: SWZ 21-Sep-2007: If we knew, a priori, the value of n, we
        // could avoid building B and just compute "Nbig" from Duj2a as we
        // went.  Duj2a uses q = n / N.  The value of q is approximated by
        // the sampling rate, r.  That is, q =~ r because the exact mechanics
        // of the sampling may cause variations.  In particular, Bernoulli
        // sampling returns a row with probability r and so the number of
        // items in the sample is rarely exactly r * N.  In LucidDB system
        // sampling, n != r * N is possible when r * N would return less rows
        // than sampling clumps or when r * N is rounded to the next nearest
        // integer.  So for now, store the values of B and compute q from n
        // once we know it.

        // Build special data set for Duj2a computation.
        if (classSize > Duj2a_DIVIDER) {
            B.add(classSize);
        } else {
            n_small += classSize;
            dn_small++;
            f_small.increment(classSize);
        }
    }

    /**
     * Returns the sample size.
     */
    public long getSampleSize()
    {
        return n;
    }

    /**
     * Returns the number of classes (distinct values) in the sample.
     */
    public long getNumSampleClasses()
    {
        return dn;
    }

    /**
     * Estimates the cardinality of the population with the assumption that all
     * classes except the null class are distinct.
     */
    public long estimateDistinctWithNullClass()
    {
        double q = (double) n / (double) N;

        long N_null = Math.round((double) nullClassSize / q);

        if (N_null >= N) {
            // Assume all classes were found in the sample.
            return dn;
        }

        return N - N_null + 1;
    }

    /**
     * Estimates the cardinality of the population using the formulas described
     * {@link FarragoCardinalityEstimator above}.
     */
    public long estimate()
    {
        if (n == 0) {
            return 0;
        } else if (assumeUniqueConstraint) {
            if (nullClassSize > 0) {
                return estimateDistinctWithNullClass();
            } else {
                return N;
            }
        }

        // f1 is the number of distinct values in the sample with frequency 1
        long f1 = f.get(1);

        if (f1 == dn) {
            // All samples were frequency 1: assume uniqueness in the
            // population and short circuit.  Note that without this short
            // circuit, the hybrid estimator would return N anyway.  Why?
            // In this case f1 = dn = n.  The equation for Duj1 is
            // (n * dn) / (n - f1 + (f1 * q)).  Replacing f1 and dn
            // with n, we have (n * n) / (n - n + (n * q)) = n / q.  Replacing
            // q with n / N, we get Duj1 = N.  Similarly, ysqD = 0.0 (the
            // summation term in Eq. 16 is zero since only fi for all
            // i != 1 is 0).  Therefore the Duj2 estimator will be chosen.
            // It's first term is Duj1 / dn = N / n.  The second term
            // is dn (which equals n) because ysqD = 0.  Therefore,
            // Duj2 = (N / n) * n = N.  If this short circuit is triggered
            // erroneously, it's usually an indication that the sample size
            // was too small.
            return N;
        }

        // Determine q: number of samples (n) divided by total
        // population N (where N == population size)
        double q = (double) n / (double) N;

        // Estimate D as Duj1, via eq 11:
        double Duj1_ = computeDuj1(n, dn, f1, q);
        long Duj1 = Math.round(Duj1_);

        // Estimate class size variance (variance in Nj, the number of members
        // of each class) from D
        double ysqD = estimateVarianceFromD(n, N, f, Duj1);

        // Choose algorithm:
        // if ysqD < alpha1 = 0.9: use Duj2
        // if alpha1 <= ysqD < alpha2 = 30; use Duj2a
        // else use Dsh3

        long D;
        if (ysqD < 0.9) {
            D = computeDuj2(n, dn, f1, q, ysqD);
        } else if (ysqD < 30.0) {
            D = computeDuj2a(f1, q, ysqD);
        } else {
            D = computeDsh3(q);
        }

        // For sanity, clamp result to range [dn, N]: there can't be fewer
        // distinct values in the full population than in the sample and there
        // can't be more distinct values than the size of the population.
        D = clamp(D, dn, N);

        return D;
    }

    private long clamp(long value, long min, long max)
    {
        if (value > max) {
            value = max;
        } else if (value < min) {
            value = min;
        }
        return value;
    }

    private static double estimateVarianceFromD(
        long n,
        long N,
        SparseLongArray f,
        long D)
    {
        // Estimate gamma^2(D): the variance of the class sizes Nj Eq 16: y^2(D)
        // = max(0, D/n^2 * sum(i = 1 to n: i*(i - 1)*fi) + (D/N) - 1
        long sum = 0;
        for (long i = 1; i <= n; i++) {
            long fi = f.get(i);
            sum += i * (i - 1) * fi;
        }

        long nsq = n * n;

        double term1 = ((double) D / (double) nsq) * (double) sum;
        double term2 = (double) D / (double) N;
        double ysqD = Math.max(0.0, term1 + term2 - 1.0);
        return ysqD;
    }

    private static double computeDuj1(long n, long dn, long f1, double q)
    {
        // Eq 11: (1 - ((1 - q) * f1) / n)^(-1) * dn which can be reduced to:
        // (n * dn) / (n - f1 + (f1 * q))

        double numerator = (double) n * (double) dn;
        double denominator = (double) (n - f1) + ((double) f1 * q);

        double Duj1_ = numerator / denominator;
        return Duj1_;
    }

    private static long computeDuj2(
        long n,
        long dn,
        long f1,
        double q,
        double ysqD)
    {
        // Term 1: (1 - (f1 * (1 - q) / n)) ^ -1, or:
        // n / (n - f1 + (f1 * q))

        double term1 = (double) n / ((double) (n - f1) + ((double) f1 * q));

        // Term 2: dn - ((f1 * (1 - q) * ln(1 - q) * ysqD) / q)

        double numerator = (double) f1 * (1.0 - q) * Math.log(1.0 - q) * ysqD;

        double term2 = (double) dn - (numerator / q);

        return Math.round(term1 * term2);
    }

    private long computeDuj2a(long f1, double q, double ysqD)
    {
        // Compute Duj2; where all classes whose frequency in the sample
        // exceeds Duj2a_DIVIDER are removed.
        // If there are none, just return Duj2.

        // if n = 0 when B is removed, fall back on Duj2.
        if (n_small == 0) {
            return computeDuj2(n, dn, f1, q, ysqD);
        }

        long f1_small = f_small.get(1);

        if (f1_small == dn_small) {
            // Assume all classes except those in B are unique.

            // Estimate how many rows in the population belong the classes in B
            long Nbig = 0;
            for (Long vc : B) {
                Nbig += vc;
            }
            Nbig = Math.round((double) Nbig / q);

            if (Nbig >= N) {
                // Very few classes other than those in B.  Assume we found all
                // the classes in our sample.
                return dn;
            }

            // N - Nbig is the number of rows in the population not represented
            // by classes in B.  We assume each of these other rows in a
            // unique class.
            return B.size() + (N - Nbig);
        }

        // Generate an estimate of the population size when the classes in B
        // are removed by estimating Nj for each class in B and subtracting
        // that from N.

        long N_small = N;
        for (Long vc : B) {
            // nj = (q * Nj) / (1 - (1 - q)^Nj)
            // We know nj and q: solve for Nj numerically
            long Nj = estimateNj(vc, q);

            N_small -= Nj;
        }

        // Compute q, Duj1 and gamma-squared(D) for the sample containing only
        // the infrequent classes.
        double q_small = (double) n_small / (double) N_small;
        double Duj1_small_ = computeDuj1(n_small, dn_small, f1_small, q_small);
        long Duj1_small = Math.round(Duj1_small_);

        double ysqD_small =
            estimateVarianceFromD(n_small, N_small, f_small, Duj1_small);

        long D_small =
            computeDuj2(n_small, dn_small, f1_small, q_small, ysqD_small);

        return D_small + B.size();
    }

    private long estimateNj(final long nj, final double q)
    {
        // To estimate Nj.

        // 1. Know that Nj >= nj
        // 2. Know that Nj <= N (rowCount)
        long minNj = nj;
        long maxNj = N;

        // Binary search our way to a best Nj value.  Could do this in
        // floating point and come within epsilon of the correct value,
        // but I think we just want to round to the nearest integer.

        final double _1q = 1.0 - q;
        final double njTarget = (double) nj;

        long bestNj = -1;
        long prevBestNj = -1;
        double bestNjDelta = Double.MAX_VALUE;
        do {
            long Nj = ((maxNj - minNj) / 2) + minNj;

            // nj = (q * Nj) / (1 - (1 - q)^Nj)
            double njGuess = compute_nj(q, _1q, (double) Nj);

            if (njGuess == njTarget) {
                // Nailed it.
                return Nj;
            }

            double delta = njTarget - njGuess;
            if (Double.isNaN(delta)) {
                throw new RuntimeException("Got NaN");
            }

            int sign = (int) Math.signum(delta);
            delta = Math.abs(delta);

            if (sign < 0) {
                // Guess is too high
                maxNj = Nj;
            } else if (sign > 0) {
                // Guess it too low
                minNj = Nj;
            } else {
                // Shouldn't be possible.
                assert (false);
                return Nj;
            }

            if (delta <= 1.0) {
                // Very close.
                if (sign < 0) {
                    if ((Nj - 1) < minNj) {
                        return Nj;
                    }

                    // Make one last attempt at Nj - 1
                    minNj = Nj - 1;
                } else {
                    // sign > 0
                    if ((Nj + 1) > maxNj) {
                        return Nj;
                    }

                    maxNj = Nj + 1;
                }
            }

            if (delta < bestNjDelta) {
                prevBestNj = bestNj;

                bestNjDelta = delta;
                bestNj = Nj;
            }
        } while ((maxNj - minNj) > 1);

        if (bestNj == minNj) {
            if (prevBestNj == maxNj) {
                // Just tried maxNj, don't try it again.
                return bestNj;
            }

            // Try maxNj to make sure.
            // nj = (q * Nj) / (1 - (1 - q)^Nj)
            double njGuess = compute_nj(q, _1q, (double) maxNj);

            if (Math.abs(njTarget - njGuess) < bestNjDelta) {
                return maxNj;
            }
        } else if (bestNj == maxNj) {
            if (prevBestNj == minNj) {
                // Just tried minNj, don't try it again.
                return bestNj;
            }

            // Try minNj to make sure
            // Try maxNj to make sure.
            // nj = (q * Nj) / (1 - (1 - q)^Nj)
            double njGuess = compute_nj(q, _1q, (double) minNj);

            if (Math.abs(njTarget - njGuess) < bestNjDelta) {
                return minNj;
            }
        } else {
            assert (false);
        }

        return bestNj;
    }

    private double compute_nj(
        final double q,
        final double _1q,
        double Nj)
    {
        return (q * Nj) / (1.0 - Math.pow(_1q, Nj));
    }

    private long computeDsh3(double q)
    {
        double t1num = 0.0;
        double t1den = 0.0;
        double t2num = 0.0;
        double t2den = 0.0;

        double qsq = q * q;
        double _1qsq = 1.0 - qsq;
        double _1q = 1.0 - q;
        double _q1 = 1.0 + q;

        for (long i = 1; i <= n; i++) {
            double fi = (double) f.get(i);

            double id = (double) i;

            double _1qi = Math.pow(_1q, id);

            double id_1 = (double) (i - 1);

            t1num += id * qsq * Math.pow(_1qsq, id_1) * fi;

            t1den += _1qi * (Math.pow(_q1, id) - 1.0) * fi;

            t2num += _1qi * fi;

            t2den += id * q * Math.pow(_1q, id_1) * fi;
        }

        double f1 = (double) (f.get(1));

        double D =
            (double) dn
            + (f1 * (t1num / t1den) * ((t2num / t2den) * (t2num / t2den)));

        return Math.round(D);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class SparseLongArray
    {
        private final long GENERIC_THRESHOLD = 10;

        /**
         * Storage for the first N ({@link #GENERIC_THRESHOLD}) indices.
         */
        private final long [] base;

        /**
         * Generic storage for any other index;
         */
        private final Map<Long, Long> generic;

        public SparseLongArray()
        {
            this.base = new long[(int) GENERIC_THRESHOLD];
            this.generic = new HashMap<Long, Long>();
        }

        public long get(long index)
        {
            if (index < GENERIC_THRESHOLD) {
                return base[(int) index];
            } else {
                Long value = generic.get(index);
                if (value == null) {
                    return 0;
                }
                return value;
            }
        }

        public void set(long index, long value)
        {
            if (index < GENERIC_THRESHOLD) {
                base[(int) index] = value;
            } else {
                generic.put(index, value);
            }
        }

        public void increment(long index)
        {
            if (index < GENERIC_THRESHOLD) {
                base[(int) index]++;
            } else {
                set(index, get(index) + 1);
            }
        }
    }
}

// End FarragoCardinalityEstimator.java
