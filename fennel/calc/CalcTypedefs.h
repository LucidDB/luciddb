/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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

#ifndef Fennel_CalcTypedefs_Included
#define Fennel_CalcTypedefs_Included

FENNEL_BEGIN_NAMESPACE

/**
 * CalcYYLocType provides the location of the token or expression being
 * parsed.
 */
typedef struct 
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
  int first_pos;
  int last_pos;
} CalcYYLocType;


typedef long TProgramCounter;

FENNEL_END_NAMESPACE

#endif

// End CalcTypedefs.h
