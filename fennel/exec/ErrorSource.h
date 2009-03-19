/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_ErrorSource_Included
#define Fennel_ErrorSource_Included

#include "fennel/exec/ErrorTarget.h"
#include "fennel/tuple/TupleAccessor.h"

#include <sstream>

FENNEL_BEGIN_NAMESPACE

/**
 * ErrorSource is a common base for all classes that post row exceptions to
 * an ErrorTarget. An ErrorSource generally corresponds to an ExecStream.
 * Fennel data is typiclly interpreted as external data via separate
 * metadata, so it is usually required for an ErrorSource to be
 * preregistered with the external system. (For example, a Fennel long might
 * map to a decimal or datetime type.)
 */
class ErrorSource
{
    SharedErrorTarget pErrorTarget;
    std::string name;

    /**
     * Tuple accessor for error records handed by this error source
     */
    TupleAccessor errorAccessor;

    /**
     * A buffer for error records
     */
    boost::shared_ptr<FixedBuffer> pErrorBuf;

protected:
    /**
     * Constructs a new uninitialized ErrorSource.
     */
    explicit ErrorSource();

    /**
     * Constructs a new ErrorSource.
     *
     * @param pErrorTarget the ErrorTarget to which errors will be posted,
     * or NULL to ignore errors.
     *
     * @param name the unique name of this source, such as the name of
     *   a Fennel ExecStream
     */
    explicit ErrorSource(
        SharedErrorTarget pErrorTarget,
        const std::string &name);

public:
    virtual ~ErrorSource();

    /**
     * For use when initialization has to be deferred until after construction.
     *
     * @param pErrorTarget the ErrorTarget to which errors will be posted
     *
     * @param name the name of this source
     */
    virtual void initErrorSource(
        SharedErrorTarget pErrorTarget,
        const std::string &name);

    /**
     * Posts an exception, such as a row exception.
     *
     * @see ErrorTarget for a description of the parameters
     */
    void postError(
        ErrorLevel level, const std::string &message,
        void *address, long capacity, int index);

    /**
     * Posts an exception, such as a row exception.
     *
     * @see ErrorTarget for a description of the parameters
     */
    void postError(
        ErrorLevel level, const std::string &message,
        const TupleDescriptor &errorDesc, const TupleData &errorTuple,
        int index);

    /**
     * @return true iff an error target has been set
     */
    bool hasTarget() const
    {
        return pErrorTarget.get() ? true : false;
    }

    /**
     * @return the ErrorTarget for this source
     */
    ErrorTarget &getErrorTarget() const
    {
        assert(hasTarget());
        return *(pErrorTarget.get());
    }

    /**
     * @return the SharedErrorTarget for this source
     */
    SharedErrorTarget getSharedErrorTarget() const
    {
        return pErrorTarget;
    }

    /**
     * Gets the name of this source. Useful to construct nested names for
     * subcomponents that are also ErrorSources.
     * @return the name
     */
    std::string getErrorSourceName() const
    {
        return name;
    }

    /**
     * Sets the name of this source. Useful to construct dynamic names for
     * fine-grained filtering.
     */
    void setErrorSourceName(std::string const& n)
    {
        name = n;
    }

    void disableTarget();
};

FENNEL_END_NAMESPACE

#endif

// End ErrorSource.h
