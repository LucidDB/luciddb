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
//
// RegisterReference
//
*/
#ifndef Fennel_RegisterReference_Included
#define Fennel_RegisterReference_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "boost/lexical_cast.hpp"

#include <strstream>

FENNEL_BEGIN_NAMESPACE

using namespace std;
using boost::lexical_cast;

typedef TupleData ** TRegisterSetP;
typedef TupleDescriptor ** TRegisterSetDescP;
typedef uint32_t TRegisterRefProp;
class Calculator;



//! A reference to a register. Base class provides
//! non-templated interface to RegisterRef.
//!
//! Optionally optimizes subsequent reads and writes and resets pointers.
//! 
//! To provide different properties in RegisterReferences, such as
//! read-only access, and resetability, in a "clean" way would require
//! run-time-polymorphism in the Instruction tree. At this point the
//! extra overhead of function pointers and vtables doesn't seem like 
//! the best choice for this performance critical object. So, for
//! the moment, object properties will be internal-state-based vs.
//! object-type-based.
class RegisterReference
{
public:
    //! Index all register sets
    enum ERegisterSet {
        EFirstSet = 0,  
        ELiteral =  0,
        EInput =    1,
        EOutput =   2,
        ELocal =    3,
        EStatus =   4,
        ELastSet,           //!< Insert new register set before this entry.
        EUnknown =  1000
    };

    //! Constructor for an invalid RegisterReference
    explicit
    RegisterReference()
        : mSetIndex(EUnknown),
          mIndex(0),
          mType(STANDARD_TYPE_END),
          mPData(0),
          mCbData(0),
          mCbStorage(0)
    { 
        mProp = EPropNone;
    }

    //! Constructor for a valid RegisterReference
    //!
    //! @param set Register set
    //! @param index Register with a given set
    //! @param datatype The type of the underlying data.
    explicit
    RegisterReference(ERegisterSet set,
                      unsigned long index,
                      StandardTypeDescriptorOrdinal datatype)
        : mSetIndex(set),
          mIndex(index),
          mType(datatype),
          mPData(0),
          mCbData(0),
          mCbStorage(0)
    {
        assert(mSetIndex < ELastSet);
        assert(mSetIndex >= EFirstSet);
        setDefaultProperties();
    }
  
    virtual
    ~RegisterReference() { }


    //! Properties control behavior of RegisterReference
    //!
    //! Note: Values are powers of two to allow bitmasking.
    enum EProperties {
        EPropNone         = 0,  //!< No properties set
        EPropReadOnly     = 1,  //!< Prevent writes and re-pointing pData.
                                //! Enforced only through assert()
        EPropCachePointer = 2,  //!< Keep a pointer to data instead of
                                //! just in time indexing.
        EPropPtrReset     = 4   //!< Journal pointer changes to allow resetting.
    };

    // TODO: Replace with array indexed by ERegisterSet
    //! Literal set can be cached, and really should be read only.
    static const uint32_t KLiteralSetDefault = EPropReadOnly | EPropCachePointer;
    //! Input must be read only. Caching doesn't make sense for input.
    static const uint32_t KInputSetDefault   = EPropReadOnly;
    //! Caching doesn't make sense for output.
    static const uint32_t KOutputSetDefault  = EPropNone;
    //! Local set can be cached, and must reset pointers if Calculator::exec()
    //! is to be called again and NULLs are written or pointers are moved.
    static const uint32_t KLocalSetDefault   = EPropCachePointer | EPropPtrReset;
    //! Status register can be cached if it is never re-bound.
    static const uint32_t KStatusSetDefault  = EPropNone;

    //! Provide a pointer to encapsulating Calculator
    //! 
    //! Must be in .cpp file for recursive include requirement reasons.
    //! Refers to Calculator and RegisterReference objects.
    void setCalc(Calculator* calcP);

    //! Perform pre-execution optimizations
    //!
    //! If CachePointer or PtrReset properties are set, save a pointer
    //! directly to the data.
    void cachePointer();

    //! Return set index.
    ERegisterSet setIndex() const
    {
        return mSetIndex;
    }
    //! Return register index within a set.
    unsigned long index() const 
    {
        return mIndex;
    }
    //! Is this a valid RegisterReference?
    bool isValid() const
    { 
        return ((mSetIndex < ELastSet) ? true : false);
    }
    //! Return type information.
    StandardTypeDescriptorOrdinal type() const {
        return mType;
    }

    //! Returns a string describing the register set
    static inline string getSetName(ERegisterSet set)
    {
        switch (set) {
        case ELiteral:
            return "C";
        case ELocal:
            return "L";
        case EInput:
            return "I";
        case EOutput:
            return "O";
        case EStatus:
            return "S";
        default:
            throw std::invalid_argument("fennel/calc/RegisterReference::getSetName"); 
        }
    }

    //! Provide a nicely formatted string describing the register for the
    //! specifed set and index
    static inline string toString(ERegisterSet set, unsigned long index)
    {
        ostringstream ostr("");
        ostr << getSetName(set) << index;
        return ostr.str();
    }

    //! Provide a nicely formatted string describing this register.
    string toString() const;
    //! Return current value of register.
    virtual string valueToString() const = 0;
    //! Is the register currently set to NULL?
    virtual bool isNull() const = 0;

protected:
    //! Register set index
    const ERegisterSet mSetIndex;       
    
    //! Register index within a register set.
    const unsigned long mIndex;
    
    //! Underlying type of register.
    const StandardTypeDescriptorOrdinal mType;

    //! Array of pointers to register set tuples
    //!
    //! Used as starting point to index through sets and registers.
    TRegisterSetP mRegisterSetP;

    //! Array of pointers to register set descriptors
    //!
    //! Used as starting point to index through sets and registers.
    TRegisterSetDescP mRegisterSetDescP;

    //! RegisterReferences that will be reset before next exec.
    //!
    //! Points to Calculator. Appended to iff PtrReset property is set.
    vector<RegisterReference *>*mResetP;

    //! Cached pointer was set to null or moved
    bool mCachePtrModified;
    
    //! Cached and/or duplicated data pointer.
    //!
    //! Only valid if CachePointer or PtrReset property is set.
    PBuffer mPData;

    //! Cached and/or duplicated length of mPData;
    //!
    //! Only valid if CachePointer or PtrReset property is set.
    TupleStorageByteLength mCbData;

    //! Cached and/or duplicated capacity of mPData;
    //!
    //! Only valid if CachePointer or PtrReset property is set.
    TupleStorageByteLength mCbStorage;

    //! Behavior properties of this register.
    TRegisterRefProp mProp; 

    //! Define default properties for registers based on register set.
    void setDefaultProperties()
    {
        switch(mSetIndex) {
        case ELiteral:
            mProp = KLiteralSetDefault;
            break;
        case EInput:
            mProp = KInputSetDefault;
            break;
        case EOutput:
            mProp = KOutputSetDefault;
            break;
        case ELocal:
            mProp = KLocalSetDefault;
            break;
        case EStatus:
            mProp = KStatusSetDefault;
            break;
        default:
            mProp = EPropNone;
        }
    }
};

//! A typed group of accessor functions to a register
template<typename TMPLT>
class RegisterRef : public RegisterReference
{
public:
    explicit
    //! Create an invalid object.
    RegisterRef () : RegisterReference()
    { }

    //! Create a valid register reference
    //!
    //! @param set Register set
    //! @param index Register with a given set
    //! @param datatype The type of the underlying data.
    explicit
    RegisterRef(ERegisterSet set,
                unsigned long index,
                StandardTypeDescriptorOrdinal datatype)
        : RegisterReference(set, index, datatype)
    { }

    //! get/peek/read a value from a register
    //!
    //! Assumes register is not null.
    TMPLT value() const {
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            assert(mPData);
            return *(reinterpret_cast<TMPLT*>(mPData));
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetP[mSetIndex]);
            TupleData* tupleDataP = mRegisterSetP[mSetIndex];
            TupleDatum* datumP = &((*tupleDataP)[mIndex]);
            assert(datumP->pData); // Cannot read from a NULL value.
            return *(reinterpret_cast<TMPLT*>(const_cast<PBuffer>(datumP->pData)));
        }
    }


    //! put/poke/set a value into a register
    //!
    //! Assumes register is not null.
    void
    value(TMPLT newV)
    {
        assert(!(mProp & EPropReadOnly));
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            assert(mPData);
            *(reinterpret_cast<TMPLT*>(mPData)) = newV;
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetP[mSetIndex]);
            TupleData* tupleDataP = mRegisterSetP[mSetIndex];
            TupleDatum* datumP = &((*tupleDataP)[mIndex]);
            assert(datumP->pData); // Cannot write into a NULL value.
            *(reinterpret_cast<TMPLT*>(const_cast<PBuffer>(datumP->pData))) = newV;
        }
    }

    //! Set a register to null.
    //!
    //! Will append to mResetP to allow register to be reset.
    void toNull() {
        assert(!(mProp & EPropReadOnly));
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            if ((mProp & EPropPtrReset) && !mCachePtrModified) {
                mCachePtrModified = true;
                mResetP->push_back(this);
            }
            mPData = NULL;
            mCbData = 0;
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetP[mSetIndex]);
            assert(!(mProp & EPropReadOnly));
            TupleData* tupleDataP = mRegisterSetP[mSetIndex];
            (*tupleDataP)[mIndex].pData = NULL;
        }
        
    }
    //! Check if register is null.
    bool isNull() const {
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            return (mPData ? false : true );
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetP[mSetIndex]);
            TupleData* tupleDataP = mRegisterSetP[mSetIndex];
        
            TupleDatum* datumP = &((*tupleDataP)[mIndex]);
            return (datumP->pData ? false : true );
        }
        
    }
    //! Get pointer value, rather than what pointer references.
    //!
    //! Used by PointerInstruction, where TMPLT is a pointer 
    //! type, never in other Instruction types, where TMPLT
    //! is not a pointer.
    TMPLT
    pointer() const {
        assert(StandardTypeDescriptor::StandardTypeDescriptor::isArray(mType));
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            assert(mPData);  // useful or harmful?
            return reinterpret_cast<TMPLT>(mPData);
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetP[mSetIndex]);
            TupleData* tupleDataP = mRegisterSetP[mSetIndex];
            TupleDatum* datumP = &((*tupleDataP)[mIndex]);
            assert(datumP->pData);
            return reinterpret_cast<TMPLT>(const_cast<PBuffer>(datumP->pData));
        }
    }
    //! Set pointer value, rather than what pointer references.
    //!
    //! Used by PointerInstruction, where TMPLT is a pointer 
    //! type, never in other Instruction types, where TMPLT
    //! is not a pointer.
    //! Will append to mResetP to allow register to be reset.
    void
    pointer(TMPLT newP, TupleStorageByteLength len)
    {
        assert(!(mProp & EPropReadOnly));
        assert(newP);  // use toNull()
        assert(StandardTypeDescriptor::StandardTypeDescriptor::isArray(mType));
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            if ((mProp & EPropPtrReset) && !mCachePtrModified) {
                mCachePtrModified = true;
                mResetP->push_back(this);
            }
            reinterpret_cast<TMPLT>(mPData) = newP;
            mCbData = len;
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetP[mSetIndex]);
            TupleData* tupleDataP = mRegisterSetP[mSetIndex];
            TupleDatum* datumP = &((*tupleDataP)[mIndex]);
            reinterpret_cast<TMPLT>(const_cast<PBuffer>(datumP->pData)) = newP;
            datumP->cbData = len; 
            
        }
    }

    //! Get length, in bytes, of data buffer
    //!
    //! This is the actual length of the object pointed to, not
    //! the amount of memory allocated for the object. For example,
    //! this could be the length, in bytes, of a VARCHAR string.
    TupleStorageByteLength
    length() const
    {
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            return mCbData;
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetP[mSetIndex]);
            TupleData* tupleDataP = mRegisterSetP[mSetIndex];
            TupleDatum* datumP = &((*tupleDataP)[mIndex]);
            return datumP->cbData;
        }
    }
    //! Get storage length / maximum length, in bytes, of data buffer.
    //!
    //! Note that there is no corresponding write storage(arg). Calculator
    //! considers cbStorage to be read-only information. Calculator
    //! can change neither the defined column width nor the amount
    //! of memory allocated.
    TupleStorageByteLength
    storage() const
    {
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            return mCbStorage;
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetDescP[mSetIndex]);
            //Next 3 lines clarify complex 4th line:
            //TupleDescriptor* tupleDescP = mRegisterSetDescP[mSetIndex];
            //TupleAttributeDescriptor* attrP = &((*tupleDescP)[mIndex]);
            //return attrP->cbStorage;
            return ((*(mRegisterSetDescP[mSetIndex]))[mIndex]).cbStorage;
        }
    }
    //! Get length of string, in bytes, based on string type.
    //!
    //! Fixed width, CHAR, BINARY:  Returns storage()
    //! Variable width, VARCHAR, VARBINARY: Returns length()
    TupleStorageByteLength
    stringLength() const
    {
        assert(StandardTypeDescriptor::isArray(mType));
        if (StandardTypeDescriptor::isVariableLenArray(mType)) {
            return length();
        } else {
            return storage();
        }
    }
    //! Set length, in bytes, of data buffer.
    //!
    //! This is the actual length of the object pointed to, not
    //! the amount of memory allocated for the object. For example,
    //! this could be the length, in bytes, of a VARCHAR string.
    void
    length(TupleStorageByteLength newLen)
    {
        if (mProp & (EPropCachePointer|EPropPtrReset)) {
            assert(newLen == 0 ? true : (mPData == 0 ? false : true)); // useful or harmful?
            assert(newLen <= mCbStorage);         // useful or harmful?
            mCbData = newLen;
        } else {
            assert(mRegisterSetP);
            assert(mSetIndex < ELastSet);
            assert(mRegisterSetP[mSetIndex]);
            assert(newLen <= ((*(mRegisterSetDescP[mSetIndex]))[mIndex]).cbStorage);
            TupleData* tupleDataP = mRegisterSetP[mSetIndex];
            TupleDatum* pDatum = &((*tupleDataP)[mIndex]);
            pDatum->cbData = newLen;
        }
    }
    //! Set length of string, in bytes, based on string type.
    //!
    //! Fixed width, CHAR/BINARY: has no effect.
    //! Variable width, VARCHAR/VARBINARY: acts as a length(arg) wrapper.
    void
    stringLength(TupleStorageByteLength newLen)
    {
        assert(StandardTypeDescriptor::isArray(mType));
        if (StandardTypeDescriptor::isVariableLenArray(mType)) {
            length(newLen);
        }
    }

    //! Return a nicely formatted string representing register's
    //! current value
    //!
    //! Note: Currently does not support pointer/array types like VARCHAR.
    string
    valueToString() const {
        if (this->isNull()) { // does assert checking for us
            return "NULL";
        }
        if (StandardTypeDescriptor::StandardTypeDescriptor::isArray(mType)) {
#if 0
            // Does not compile due to ptr/non-ptr compile issue
            // TODO: Make this work with VARBINARY and I18N
            // TODO: Remove the reinterpret_cast. Ugly.
            string ret((char *)(this->pointer()), this->getS());
            return ret;
#endif
            return "Unimpl";
        }
        else {
            // TODO: Remove boost call, use stream instead
            return boost::lexical_cast<std::string>(this->value());
        }
    }
protected:
    
};

FENNEL_END_NAMESPACE

#endif

// End RegisterReference.h
