/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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

#ifndef Fennel_SebCmdInterpreter_Included
#define Fennel_SebCmdInterpreter_Included

#include "fennel/farrago/CmdInterpreter.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SebCmdInterpreter extends CmdInterpreter with storage engine bridging.
 *
 * @author John Sichi
 * @version $Id$
 */
class SebCmdInterpreter : public CmdInterpreter
{
    static unsigned short userId;
    static unsigned short dbId;

    virtual void visit(ProxyCmdOpenDatabase &);
    virtual void visit(ProxyCmdCloseDatabase &);
    virtual void visit(ProxyCmdCreateIndex &);
    virtual void visit(ProxyCmdBeginTxn &);
    virtual void visit(ProxyCmdCommit &);
    virtual void visit(ProxyCmdRollback &);

public:
    static inline unsigned short getUserId()
    {
        return userId;
    }

    static inline unsigned short getDbId()
    {
        return dbId;
    }
};

FENNEL_END_NAMESPACE

#endif

// End SebCmdInterpreter.h
