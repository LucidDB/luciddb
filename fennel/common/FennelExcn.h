/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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

#ifndef Fennel_FennelExcn_Included
#define Fennel_FennelExcn_Included

FENNEL_BEGIN_NAMESPACE

/**
 * Base class for all Fennel exceptions.
 */
class FennelExcn : public std::exception
{
protected:
    std::string msg;
    
public:
    /**
     * Constant for return value of what().
     */
    static ParamVal RTTI_WHAT_FennelExcn;
    
    /**
     * Construct a new FennelExcn.
     *
     * @param msgInit message
     */
    explicit FennelExcn(std::string const &msgInit);

    virtual ~FennelExcn() throw();

    std::string const &getMessage()
    {
        return msg;
    }
    
    /**
     * Override std::exception.  This returns "FennelExcn"; we make use of this
     * as a substitute for RTTI, which is incompatible with JNI.
     */
    virtual const char * what() const throw();
};

FENNEL_END_NAMESPACE

#endif

// End FennelExcn.h
