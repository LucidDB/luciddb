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

#ifndef Fennel_SamplingExecStreamGenerator_Included
#define Fennel_SamplingExecStreamGenerator_Included

#include "fennel/test/ExecStreamGenerator.h"
#include "fennel/lcs/LcsRowScanExecStream.h"
#include <math.h>

FENNEL_BEGIN_NAMESPACE

/**
 * Bernoulli sampling data generator.  Produces rows from the input based
 * on the Bernoulli sampling algorithm.
 *
 * @author Stephan Zuercher
 */
class BernoulliSamplingExecStreamGenerator
    : public MockProducerExecStreamGenerator
{
protected:
    boost::shared_ptr<MockProducerExecStreamGenerator> generator;

    boost::scoped_ptr<BernoulliRng> rng;

    uint nColumns;
    uint iChildRow;
    uint iLastRow;
public:
    explicit BernoulliSamplingExecStreamGenerator(
        boost::shared_ptr<MockProducerExecStreamGenerator> const &generatorInit,
        float prob, uint seed, uint nColumnsInit)
        : generator(generatorInit),
          rng(new BernoulliRng(prob)),
          nColumns(nColumnsInit),
          iChildRow((uint) -1),
          iLastRow((uint) -1)
    {
        rng->reseed(seed);
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        if (iRow != iLastRow) {
            assert(iCol == 0);

            iChildRow++;
            while (!rng->nextValue()) {
                for (int i = 0; i < nColumns; i++) {
                    generator->generateValue(iChildRow, i);
                }
                iChildRow++;
            }
            iLastRow = iRow;
        }

        return generator->generateValue(iChildRow, iCol);
    }
};

class SystemSamplingExecStreamGenerator
    : public MockProducerExecStreamGenerator
{
protected:
    boost::shared_ptr<MockProducerExecStreamGenerator> generator;

    uint nColumns;
    uint iChildRow;
    uint iLastRow;

    uint clumpSize;
    uint clumpDistance;
    uint clumpPos;

public:
    explicit SystemSamplingExecStreamGenerator(
        boost::shared_ptr<MockProducerExecStreamGenerator> const &generatorInit,
        float rate, uint nRows, uint nColumnsInit, uint nClumps)
        : generator(generatorInit),
          nColumns(nColumnsInit),
          iChildRow((uint) -1),
          iLastRow((uint) -1),
          clumpPos((uint) -1)
    {
        uint sampleSize = (uint)round((double)nRows * (double)rate);
        clumpSize = (uint)round((double)sampleSize / (double)nClumps);
        clumpDistance =
            (uint)round((double)(nRows - sampleSize) / (double)(nClumps - 1));

        uint rowsRequired =
            (clumpSize + clumpDistance) * (nClumps - 1) + clumpSize;
        if (rowsRequired > nRows && clumpDistance > 0) {
            clumpDistance--;
        }

//        std::cout << "sampleSize " << sampleSize << std::endl;
//        std::cout << "clumpSize " << clumpSize << std::endl;
//        std::cout << "clumpDistance " << clumpDistance << std::endl;
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        if (iRow != iLastRow) {
            assert(iCol == 0);

            iChildRow++;
            clumpPos++;

            if (clumpPos >= clumpSize) {
                // Skip clumpDistance rows
                for (uint i = 0; i < clumpDistance; i++) {
//                    std::cout << "skip " << iChildRow << std::endl;
                    for (int j = 0; j < nColumns; j++) {
                        generator->generateValue(iChildRow, j);
                    }
                    iChildRow++;
                }
                clumpPos = 0;
            }
            iLastRow = iRow;

//            std::cout << "gen " << iChildRow << std::endl;
        }

        return generator->generateValue(iChildRow, iCol);
    }
};

FENNEL_END_NAMESPACE

#endif

// End SamplingExecStreamGenerator.h
