/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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

#ifndef Fennel_BBPort_Included
#define Fennel_BBPort_Included

FENNEL_BEGIN_NAMESPACE

// This file defines some types and macros used in the Broadbase code.
// Code being ported can include it temporarily until all references to these
// constructs have been cleaned up.

typedef int INT;
typedef uint8_t BYTE;
typedef BYTE *PBYTE;
typedef bool BOOL;
typedef int16_t SWORD;
typedef uint32_t DWORD;
typedef DWORD HANDLE;
typedef uint32_t ULONG;
typedef int32_t LONG;
typedef int64_t INT64;
typedef uint16_t WORD;
typedef WORD *PWORD;
typedef uint UINT;
typedef char TCHAR;
typedef char BCHAR;
typedef UINT *PUINT;
typedef TCHAR BBCHAR;
typedef TCHAR *PBBCHAR;
typedef unsigned char UCHAR;
typedef int16_t INT16;
typedef int32_t INT32;
typedef int64_t INT64;
typedef float REAL32;
typedef double REAL64;

typedef INT16 SMALLINT;
typedef INT32 INTEGER;
typedef INT64 BIGINT;
typedef INT64 TIMESTAMP;
typedef WORD OFFSET, *POFFSET;
typedef DWORD RID, *PRID;

#define FALSE false
#define TRUE true

enum BBRC
{
    // Success
    BBRC_SUCCESS = 0,

    // Success-with-information
    BBRC_ENDOFDATA,
    BBRC_OUTOFSPACE,
    BBRC_ERR_NOTFOUND,

    // Error
    BBRC_ERR,
    BBRC_ERR_INTERNAL,
    BBRC_ERR_MEM_OUT,
    BBRC_ERR_ALLOC_ERROR_STACK_BUFFER,

    // todo: obsolete all following codes
    BBRC_ERR_FILEIO,
    BBRC_ERR_FILEIO_UNKNOWN,
    BBRC_ERR_FILEIO_FILE_NOT_ACTIVE,
    BBRC_ERR_FILEIO_FILE_EXISTS,
    BBRC_ERR_FILEIO_MAX_EXTENT,
    BBRC_ERR_FILEIO_FILEFULL,
};

// Fennel uses exception handling,  so these macros are dummied out
#define BB_PRELUDE()
#define BB_FINALE()
#define BB_RETURN() return
#define BB_DROP(rc, label)

FENNEL_END_NAMESPACE

#endif

// End BBPort.h
