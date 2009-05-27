/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_LogicalTxnParticipantFactory_Included
#define Fennel_LogicalTxnParticipantFactory_Included

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;

/**
 * LogicalTxnParticipantFactory defines an interface for reconstructing
 * instances of LogicalTxnParticipant during recovery.
 */
class FENNEL_TXN_EXPORT LogicalTxnParticipantFactory
{
public:
    virtual ~LogicalTxnParticipantFactory();

    /**
     * Recovers a LogicalTxnParticipant from the log.  Using the classId to
     * determine the participant type to create, the factory reads required
     * constructor parameters from the log input stream.  The factory may peool
     * participant instances; i.e. when the same constructor parameters are
     * encountered a second time, the factory can return the same instance.
     * (TODO:  refine this when parallelized recovery is implemented.)  The
     * implementation must consume ALL log data for this record, even if some
     * of it turns out to be unneeded.
     *
     * @param classId the LogicalTxnClassId recorded when the participant was
     * logged while online
     *
     * @param logStream the log information written by the participant's
     * describeParticipant() implementation
     *
     * @return reference to loaded participant
     */
    virtual SharedLogicalTxnParticipant loadParticipant(
        LogicalTxnClassId classId,
        ByteInputStream &logStream) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End LogicalTxnParticipantFactory.h
