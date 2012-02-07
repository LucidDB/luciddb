/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_BernoulliRng_Included
#define Fennel_BernoulliRng_Included

#include <boost/random.hpp>
#include <boost/cstdint.hpp>

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
class FENNEL_COMMON_EXPORT BernoulliRng
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
    void reseed(boost::uint32_t seed);

    /**
     * Returns the next value.
     */
    bool nextValue();
};

FENNEL_END_NAMESPACE

#endif

// End BernoulliRng.h
