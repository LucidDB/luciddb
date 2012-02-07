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

#ifndef Fennel_ResourceDefinition_Included
#define Fennel_ResourceDefinition_Included

#include <string>
#include <boost/format.hpp>

#include "fennel/common/ResourceBundle.h"

FENNEL_BEGIN_NAMESPACE

class FENNEL_COMMON_EXPORT ResourceDefinition
{
    const std::string _key;

public:
    explicit ResourceDefinition(
        ResourceBundle *bundle,
        const std::string &key);

    virtual ~ResourceDefinition();

    std::string format() const;

    template<typename t0>
    std::string format(const t0 &p0) const
    {
        boost::format fmt = prepareFormatter(1);

        return boost::io::str(fmt % p0);
    }

    template<typename t0, typename t1>
    std::string format(const t0 &p0, const t1 &p1) const
    {
        boost::format fmt = prepareFormatter(2);

        return boost::io::str(fmt % p0 % p1);
    }

    template<typename t0, typename t1, typename t2>
    std::string format(const t0 &p0, const t1 &p1, const t2 &p2) const
    {
        boost::format fmt = prepareFormatter(3);

        return boost::io::str(fmt % p0 % p1 % p2);
    }

    template<typename t0, typename t1, typename t2, typename t3>
    std::string format(
        const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3) const
    {
        boost::format fmt = prepareFormatter(4);

        return boost::io::str(fmt % p0 % p1 % p2 % p3);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4>
    std::string format(
        const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
        const t4 &p4) const
    {
        boost::format fmt = prepareFormatter(5);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5>
    std::string format(
        const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
        const t4 &p4, const t5 &p5) const
    {
        boost::format fmt = prepareFormatter(6);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5, typename t6>
    std::string format(
        const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
        const t4 &p4, const t5 &p5, const t6 &p6) const
    {
        boost::format fmt = prepareFormatter(7);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5 % p6);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5, typename t6, typename t7>
    std::string format(
        const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
        const t4 &p4, const t5 &p5, const t6 &p6, const t7 &p7) const
    {
        boost::format fmt = prepareFormatter(8);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5 % p6 % p7);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5, typename t6, typename t7, typename t8>
    std::string format(
        const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
        const t4 &p4, const t5 &p5, const t6 &p6, const t7 &p7,
        const t8 &p8) const
    {
        boost::format fmt = prepareFormatter(9);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5 % p6 % p7 % p8);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5, typename t6, typename t7, typename t8, typename t9>
    std::string format(
        const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
        const t4 &p4, const t5 &p5, const t6 &p6, const t7 &p7,
        const t8 &p8, const t9 &p9) const
    {
        boost::format fmt = prepareFormatter(10);

        return boost::io::str(
            fmt % p0 % p1 % p2 % p3 % p4 % p5 % p6 % p7 % p8 % p9);
    }

    // TODO: more format methods?

protected:
    boost::format prepareFormatter(int numArgs) const;

private:
    ResourceBundle *_bundle;
};

FENNEL_END_NAMESPACE

#endif // not Fennel_ResourceDefinition_Included

// End ResourceDefinition.h
