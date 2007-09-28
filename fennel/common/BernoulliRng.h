/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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

#ifndef Fennel_BernoulliRng_Included
#define Fennel_BernoulliRng_Included

#include <boost/random.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * BernoulliRng produces a series of values with a Bernoulli distribution.
 * That is, a series of values where the value true (1) appears with success
 * probability p and the values false (0) appears with failure probability
 * q = 1 - p.  This class simply combines boost::mt19937 (a Meresenne Twister
 * PRNG) with boost::bernoulli_distribution using boost::variate_generator.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
class BernoulliRng
{
private:
    /**
     * uniform random number generator
     */
    boost::mt19937 uniformRng;
    
    /**
     * Bernoulli distribution converter
     */
    boost::bernoulli_distribution<float> bernoulliDist;

    /**
     * Variate generator to produce value with Bernoulli distribution from
     * a uniform RNG.
     */
    boost::variate_generator<
        boost::mt19937 &, 
        boost::bernoulli_distribution<float> > rng;

public:
    explicit BernoulliRng(float successProbability);

    /**
     * Reseed the uniform random number generator that forms the basis of the
     * value stream.  The RNG will seed itself initially, but it's recommended
     * that you call this method with a better seed.  The usual warnings about
     * not calling this method frequently with an input of 'time(0)' apply.
     */
    void reseed(uint32_t seed);

    /**
     * Returns the next value.
     */
    bool nextValue();
};

FENNEL_END_NAMESPACE

#endif

// End BernoulliRng.h
