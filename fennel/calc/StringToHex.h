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
*/

#ifndef Fennel_StringToHex_Included
#define Fennel_StringToHex_Included

#include <string>

FENNEL_BEGIN_NAMESPACE

using std::string;

string
stringToHex(char const * const buf);

string
stringToHex(string const & s);

string
stringToHex(char const * const buf, uint buflen);


FENNEL_END_NAMESPACE

#endif
// End StringToHex.h
