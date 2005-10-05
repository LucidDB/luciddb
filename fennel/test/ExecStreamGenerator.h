/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_ExecStreamGenerator_Included
#define Fennel_ExecStreamGenerator_Included

#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include <boost/shared_ptr.hpp>
#include <algorithm>
#include <numeric>

FENNEL_BEGIN_NAMESPACE

using boost::shared_ptr;
using std::vector;

/**
 * Test data generators, usually for a 45-degree ramp
 * (output value equals input row number).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class RampExecStreamGenerator : public MockProducerExecStreamGenerator
{
protected:
    int offset;
public:
    RampExecStreamGenerator(int offset_) {
        offset = offset_;
    }

    RampExecStreamGenerator() {
        offset = 0;
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        return iRow + offset;
    }
};

/**
 * @author John V. Sichi
 */
class PermutationGenerator : public MockProducerExecStreamGenerator
{
    std::vector<int64_t> values;

public:
    explicit PermutationGenerator(uint nRows)
    {
        values.resize(nRows);
        std::iota(values.begin(), values.end(), 0);
        std::random_shuffle(values.begin(), values.end());
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        // iCol ignored
        return values[iRow];
    }
};

/**
 * A Staircase Generator.
 *
 * Outputs numbers according to the formula:
 * Height * (row / (int) Width)
 *
 * @author Wael Chatila
 * @version $Id$
 */
class StairCaseExecStreamGenerator : public MockProducerExecStreamGenerator
{
    int h;
    int w;
public:
    StairCaseExecStreamGenerator(int height, uint width) :
        h(height),
        w(width)
    {
        // empty
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        return h * (iRow / w);
    }
};

/**
 * Outputs the value of a specified dynamic param, reinterpreted as int64_t.
 *
 * @author Wael Chatila
 */
class DynamicParamExecStreamGenerator : public MockProducerExecStreamGenerator
{
    uint dynamicParamId;
    SharedDynamicParamManager paramManager;

public:
    DynamicParamExecStreamGenerator(
        uint dynamicParamId_,
        SharedDynamicParamManager paramManager_)
        : dynamicParamId(dynamicParamId_),
          paramManager(paramManager_)
    {
        // empty
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        int64_t value = *reinterpret_cast<int64_t const *>(
            paramManager->getParam(dynamicParamId).getDatum().pData);
        return value;
    }
};

/**
 * Home-grown random number generator because I can't get boost's rng to
 * compile.  The algorithm is from "The C Programming Language" by Kernighan
 * and Ritchie.
 *
 * @author Julian Hyde
 */
class RandomNumberGenerator
{
private:
    uint seed;

public:
    RandomNumberGenerator(uint seed)
    {
        setSeed(seed);
    }

    void setSeed(uint seed)
    {
        this->seed = seed;
    }

    uint next()
    {
        seed = seed * 1103515245 + 12345;
        return seed;
    }

    uint next(uint x)
    {
        return next() % x;
    }
};

/**
 * Column generator which produces values which are uniformly distributed
 * between 0 and N - 1.
 */
class RandomColumnGenerator : public ColumnGenerator<int64_t>
{
    RandomNumberGenerator rng;
    int max;

public:
    RandomColumnGenerator(int max) : rng(42), max(max)
        {}

    int64_t next() 
    {
        return rng.next(max);
    }
};

/**
 * Column generator which generates values with a Poisson distribution.
 *
 * The Poisson distribution is a statistical distribution which characterizes
 * the intervals between successive events. For example, consider a large
 * sample of a radioactive isotope with a long half-life, and a Geiger counter
 * measuring decay events. The number of events N between time t=0 and t=1 will
 * be Poisson distributed.
 *
 * This generator generates an ascending sequence of values with a given mean
 * distance between values. For example, the sequence [3, 17, 24, 39, 45] might
 * be the first five values generated if startValue = 0 and meanDistance = 10.
 *
 * The generator generates a better statistical distribution if you give it a
 * larger value of batchSize.
 *
 * @author Julian Hyde
 */
template <class T = int64_t>
class PoissonColumnGenerator : public ColumnGenerator<T>
{
    T currentValue;
    /// batch of pre-generated values
    vector<T> nextValues;
    /// position in the batch of the last value we returned
    int ordinalInBatch;
    /// upper bound of current batch, will be the lower bound of the next
    T batchUpper;
    RandomNumberGenerator rng;
    double meanDistance;

public:
    explicit PoissonColumnGenerator(
        T startValue,
        double meanDistance,
        int batchSize,
        uint seed) : rng(seed)
    {
        assert(batchSize > 0);
        assert(meanDistance > 0);
        assert(meanDistance * batchSize >= 1);
        this->batchUpper = startValue;
        nextValues.resize(batchSize);
        this->ordinalInBatch = batchSize;
        this->meanDistance = meanDistance;
    }

    virtual ~PoissonColumnGenerator()
        {}

    T next()
    {
        if (ordinalInBatch >= nextValues.size()) {
            generateBatch();
        }
        return nextValues[ordinalInBatch++];
    }

private:
    /// Populates the next batch of values.
    void generateBatch() {
        // The next batch will contain nextValues.size() values with a mean
        // inter-value distance of meanDistance, hence its values will range
        // from batchLower to batchLower + nextValues.size() * meanDistance.
        T batchLower = this->batchUpper;
        int batchRange = (int) (meanDistance * nextValues.size());
        T batchUpper = batchLower + batchRange;
        assert(batchUpper > batchLower);
        for (int i = 0; i < nextValues.size(); i++) {
            nextValues[i] = batchLower + static_cast<T>(rng.next(batchRange));
        }
        std::sort(nextValues.begin(), nextValues.end());
        this->batchUpper = batchUpper;
        this->ordinalInBatch = 0;
    }
};


/**
 * Generates a result set consisting of columns each generated by its own
 * generator.
 *
 * @author Julian Hyde
 */
class CompositeExecStreamGenerator : public MockProducerExecStreamGenerator
{
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > generators;
    uint currentRow;
    uint currentCol;

public:
    explicit CompositeExecStreamGenerator(
            vector<shared_ptr<ColumnGenerator<int64_t> > > const &generators)
        : generators(generators)
    {
        currentRow = uint(-1);
        currentCol = columnCount() - 1;
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        // Check that access is sequential.
        if (iCol == 0) {
            assert(iRow == currentRow + 1);
            assert(currentCol == columnCount() - 1);
        } else {
            assert(iRow == currentRow);
            assert(iCol == currentCol + 1);
        }
        currentRow = iRow;
        currentCol = iCol;

        return generators[iCol]->next();
    }

 private:
    uint columnCount()
    {
        return generators.size();
    }
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamGenerator.h
