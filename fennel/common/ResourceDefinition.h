/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

#ifndef Fennel_ResourceDefinition_Included
#define Fennel_ResourceDefinition_Included

#include <string>
#include <boost/format.hpp>

#include "fennel/common/ResourceBundle.h"

FENNEL_BEGIN_NAMESPACE

class ResourceDefinition
{
    const std::string _key;

public:
    explicit ResourceDefinition(ResourceBundle *bundle,
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
    std::string format(const t0 &p0, const t1 &p1, const t2 &p2,
                       const t3 &p3) const
    {
        boost::format fmt = prepareFormatter(4);

        return boost::io::str(fmt % p0 % p1 % p2 % p3);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4>
    std::string format(const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
                  const t4 &p4) const
    {
        boost::format fmt = prepareFormatter(5);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5>
    std::string format(const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
                  const t4 &p4, const t5 &p5) const
    {
        boost::format fmt = prepareFormatter(6);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5, typename t6>
    std::string format(const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
                  const t4 &p4, const t5 &p5, const t6 &p6) const
    {
        boost::format fmt = prepareFormatter(7);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5 % p6);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5, typename t6, typename t7>
    std::string format(const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
                  const t4 &p4, const t5 &p5, const t6 &p6, const t7 &p7) const
    {
        boost::format fmt = prepareFormatter(8);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5 % p6 % p7);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5, typename t6, typename t7, typename t8>
    std::string format(const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
                  const t4 &p4, const t5 &p5, const t6 &p6, const t7 &p7,
                  const t8 &p8) const
    {
        boost::format fmt = prepareFormatter(9);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5 % p6 % p7 % p8);
    }

    template<typename t0, typename t1, typename t2, typename t3, typename t4,
             typename t5, typename t6, typename t7, typename t8, typename t9>
    std::string format(const t0 &p0, const t1 &p1, const t2 &p2, const t3 &p3,
                  const t4 &p4, const t5 &p5, const t6 &p6, const t7 &p7,
                  const t8 &p8, const t9 &p9) const
    {
        boost::format fmt = prepareFormatter(10);

        return boost::io::str(fmt % p0 % p1 % p2 % p3 % p4 % p5 % p6 % p7 % p8
                              % p9);
    }

    // TODO: more format methods?

protected:
    boost::format prepareFormatter(int numArgs) const;

private:
    ResourceBundle *_bundle;
};

FENNEL_END_NAMESPACE

#endif // not Fennel_ResourceDefinition_Included
