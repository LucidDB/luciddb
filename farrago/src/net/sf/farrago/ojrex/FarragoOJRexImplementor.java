/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.ojrex;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.rex.*;


/**
 * FarragoOJRexImplementor refines {@link OJRexImplementor}
 * to provide Farrago-specific context.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoOJRexImplementor implements OJRexImplementor
{
    //~ Methods ---------------------------------------------------------------

    // implement OJRexImplementor
    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        return implementFarrago((FarragoRexToOJTranslator) translator, call,
            operands);
    }

    /**
     * Refined version of {@link OJRexImplementor#implement}.
     *
     * @param translator provides Farrago-specific translation context
     *
     * @param call the call to be translated
     *
     * @param operands call's operands, which have already
     * been translated independently
     */
    public abstract Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands);

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        // NOTE jvs 17-June-2005:  In general, we assume that if
        // an implementor is registered, it is capable of the
        // requested implementation independent of operands.
        // Implementors which need to check their operands
        // should override this method.
        return true;
    }
}


// End FarragoOJRexImplementor.java
