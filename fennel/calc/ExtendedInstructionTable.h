/*
// $Id$
// Fennel is a relational database kernel.
// (C) Copyright 2004-2004 Disruptive Tech
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public
// License as published by the Free Software Foundation; either
// version 2 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Library General Public License for more details.
//
// You should have received a copy of the GNU Library General Public
// License along with this library; if not, write to the
// Free Software Foundation, Inc., 59 Temple Place - Suite 330,
// Boston, MA  02111-1307, USA.
//
// See the LICENSE.html file located in the top-level-directory of
// the archive of this library for complete text of license.
//
// jhyde 17 January, 2004
*/

#ifndef Fennel_ExtendedInstructionTable_Included
#define Fennel_ExtendedInstructionTable_Included

#include "fennel/calc/ExtendedInstruction.h"
#include "map"

FENNEL_BEGIN_NAMESPACE

using std::string;

//! A singleton mapping of ExtendedInstruction signatures to
//! ExtendedInstruction functors.
class ExtendedInstructionTable
{
public:
    //! Registers an extended instruction and the functor which implements it.
    template <typename T>
    void add(const string &name,
             const vector<StandardTypeDescriptorOrdinal> &parameterTypes,
             T *dummy,
             typename T::Functor functor) {
        FunctorExtendedInstructionDef<T> *pDef = 
            new FunctorExtendedInstructionDef<T>(name, parameterTypes,
                                                 functor);
        _defsByName[pDef->getSignature()] = pDef;
    }

    //! Looks up an extended instruction by signature (name + argument types)
    //!
    //! Returns null if instruction not found.
    ExtendedInstructionDef* operator[] (string const &signature) {
        return _defsByName[signature];
    }

    string signatures();

private:
    map<string, ExtendedInstructionDef *> _defsByName;
};


FENNEL_END_NAMESPACE

#endif
// End ExtendedInstructionTable.h
