/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

#ifndef Fennel_StandardTypeDescriptor_Included
#define Fennel_StandardTypeDescriptor_Included

#include "fennel/tuple/StoredTypeDescriptor.h"
#include "fennel/tuple/StoredTypeDescriptorFactory.h"

FENNEL_BEGIN_NAMESPACE

/**
 * StandardDataTypeOrdinal enumerates the ordinals of the standard types
 * provided by fennel.  Order matters.  Extension data types should
 * start from EXTENSION_TYPE_MIN.
 * NOTE: Any changes must be copied into 
 * 1) enum StandardTypeDescriptorOrdinal
 * 2) net.sf.farrago.query.FennelRelUtil.convertSqlTypeNumberToFennelTypeOrdinal
 * 3) StandardTypeDescriptor class
 * 4) StoredTypeDescriptor standardTypes
 */
enum StandardTypeDescriptorOrdinal
{
    STANDARD_TYPE_MIN = 1,
    STANDARD_TYPE_INT_8 = 1,
    STANDARD_TYPE_UINT_8 = 2,
    STANDARD_TYPE_INT_16 = 3,
    STANDARD_TYPE_UINT_16 = 4,
    STANDARD_TYPE_INT_32 = 5,
    STANDARD_TYPE_UINT_32 = 6,
    STANDARD_TYPE_INT_64 = 7,
    STANDARD_TYPE_UINT_64 = 8,
    STANDARD_TYPE_BOOL = 9,
    STANDARD_TYPE_REAL = 10,
    STANDARD_TYPE_DOUBLE = 11,
    STANDARD_TYPE_CHAR = 12,
    STANDARD_TYPE_VARCHAR = 13,
    STANDARD_TYPE_BINARY = 14,
    STANDARD_TYPE_VARBINARY = 15,
    STANDARD_TYPE_END,
    EXTENSION_TYPE_MIN = 1000
};

/**
 * StandardTypeDescriptor provides convenience functions to
 * StandardTypeDescriptorOrdinal enum
 */
class StandardTypeDescriptor
{
public:
    static inline char const * const
    toString(StandardTypeDescriptorOrdinal st)
    {
        switch (st) {
        case STANDARD_TYPE_INT_8:
            return "s1";             // signed, 1 byte
        case STANDARD_TYPE_UINT_8:
            return "u1";             // unsigned, 1 byte
        case STANDARD_TYPE_INT_16:
            return "s2";
        case STANDARD_TYPE_UINT_16:
            return "u2";
        case STANDARD_TYPE_INT_32:
            return "s4";
        case STANDARD_TYPE_UINT_32:
            return "u4";
        case STANDARD_TYPE_INT_64:
            return "s8";
        case STANDARD_TYPE_UINT_64:
            return "u8";
        case STANDARD_TYPE_BOOL:
            return "bo";
        case STANDARD_TYPE_REAL:     // float
            return "r";
        case STANDARD_TYPE_DOUBLE:
            return "d";
        case STANDARD_TYPE_CHAR:
            return "c";
        case STANDARD_TYPE_VARCHAR:
            return "vc";
        case STANDARD_TYPE_BINARY:
            return "b";
        case STANDARD_TYPE_VARBINARY:
            return "vb";
        default:
            throw std::invalid_argument("fennel/tuple/StandardTypeDescriptor::toString");
        }
    }
    
    static inline StandardTypeDescriptorOrdinal
    fromString(char const * const str)
    {
        // A bit ugly, but rather fast.
        switch (*str) {
        case 's':
            switch(*(str+1)) {
            case '1':
                return STANDARD_TYPE_INT_8;
            case '2':
                return STANDARD_TYPE_INT_16;
            case '4':
                return STANDARD_TYPE_INT_32;
            case '8':
                return STANDARD_TYPE_INT_64;
            default:
                break;
            }
            break;
        case 'u':
            switch(*(str+1)) {
            case '1':
                return STANDARD_TYPE_UINT_8;
            case '2':
                return STANDARD_TYPE_UINT_16;
            case '4':
                return STANDARD_TYPE_UINT_32;
            case '8':
                return STANDARD_TYPE_UINT_64;
            default:
                break;
            }
            break;
        case 'r':
            return STANDARD_TYPE_REAL;
        case 'd':
            return STANDARD_TYPE_DOUBLE;
        case 'c':
            return STANDARD_TYPE_CHAR;
        case 'v':
            switch(*(str+1)) {
            case 'c':
                return STANDARD_TYPE_VARCHAR;
            case 'b':
                return STANDARD_TYPE_VARBINARY;
            default:
                break;
            }
        case 'b':
            switch(*(str+1)) {
            case 'o':
                return STANDARD_TYPE_BOOL;
            case 0:         // string null terminator
                return STANDARD_TYPE_BINARY;
            default:
                break;
            }
        }
        
        throw std::invalid_argument("fennel/tuple/StandardTypeDescriptor::fromString");
        return EXTENSION_TYPE_MIN;
    }
    
    static inline bool
    isNative(StandardTypeDescriptorOrdinal st)
    {
        if (st <= STANDARD_TYPE_DOUBLE) {
            return true;
        }
        return false;
    }

    /**
     * Note: Boolean considered integral native.
     * Might not be a reasonable assumption.
     */
    static inline bool
    isIntegralNative(StandardTypeDescriptorOrdinal st)
    {
        if (st <= STANDARD_TYPE_BOOL) {
            return true;
        }
        return false;
    }

    /**
     * Note: Boolean not considered exact.
     * Might not be a reasonable assumption.
     */
    static inline bool
    isExact(StandardTypeDescriptorOrdinal st)
    {
        if (st <= STANDARD_TYPE_UINT_64) {
            return true;
        }
        return false;
    }
    

    static inline bool
    isApprox(StandardTypeDescriptorOrdinal st)
    {
        if (st == STANDARD_TYPE_REAL ||
            st == STANDARD_TYPE_DOUBLE) {
            return true;
        }
        return false;
    }
    
    static inline bool
    isArray(StandardTypeDescriptorOrdinal st)
    {
        if (st >= STANDARD_TYPE_CHAR &&
            st <= STANDARD_TYPE_VARBINARY) {
            return true;
        }
        return false;
    }

    static inline bool
    isVariableLenArray(StandardTypeDescriptorOrdinal st)
    {
        if (st == STANDARD_TYPE_VARCHAR ||
            st == STANDARD_TYPE_VARBINARY) {
            return true;
        }
        return false;
    }

    static inline bool
    isFixedLenArray(StandardTypeDescriptorOrdinal st)
    {
        if (st == STANDARD_TYPE_CHAR ||
            st == STANDARD_TYPE_BINARY) {
            return true;
        }
        return false;
    }
    
private:
    explicit
    StandardTypeDescriptor();
};


/**
 * StandardTypeDescriptorFactory is an implementation of the
 * StoredTypeDescriptorFactory interface capable of constructing all of the
 * types enumerated in StandardTypeDescriptorOrdinal.
 */
class StandardTypeDescriptorFactory : public StoredTypeDescriptorFactory
{
public:
    explicit StandardTypeDescriptorFactory();
    virtual StoredTypeDescriptor const &newDataType(
        StoredTypeDescriptor::Ordinal iTypeOrdinal) const;
};

FENNEL_END_NAMESPACE

#endif

// End StandardTypeDescriptor.h
