/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
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
//
// SqlString
//
// An ascii string library that adheres to the SQL99 standard definitions
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/SqlString.h"

FENNEL_BEGIN_NAMESPACE

int
SqlStrCat(char* dest,
          int destStorageBytes,
          int destLenBytes,
          char const * const str,
          int strLenBytes)
{
    if (destLenBytes + strLenBytes > destStorageBytes) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }

    memcpy(dest + destLenBytes, str, strLenBytes);
    return destLenBytes + strLenBytes;
}


int
SqlStrCat(char* dest,
          int destStorageBytes,
          char const * const str1,
          int str1LenBytes,
          char const * const str2,
          int str2LenBytes)
{
    if (str1LenBytes + str2LenBytes > destStorageBytes) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }

    memcpy(dest, str1, str1LenBytes);
    memcpy(dest+str1LenBytes, str2, str2LenBytes);
    return str1LenBytes + str2LenBytes;
}

int
SqlStrCpy_Var(char* dest,
              int destStorageBytes,
              char const * const str,
              int strLenBytes)
{
    if (strLenBytes > destStorageBytes) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }
    memcpy(dest, str, strLenBytes);
    return strLenBytes;
}



int
SqlStrLenBit(int strLenBytes)
{
    return 8 * strLenBytes;
}

int
SqlStrLenOct(int strLenBytes)
{
    return strLenBytes;
}


FENNEL_END_NAMESPACE

