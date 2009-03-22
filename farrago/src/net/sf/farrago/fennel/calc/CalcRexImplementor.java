/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
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
package net.sf.farrago.fennel.calc;

import org.eigenbase.rex.*;


/**
 * Translates a call to a particular operator to calculator assembly language.
 *
 * <p>Implementors are held in a {@link CalcRexImplementorTable}.
 *
 * @author jhyde
 * @version $Id$
 * @since June 2nd, 2004
 */
public interface CalcRexImplementor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Generates instructions to implement this call, and returns the register
     * which holds the result.
     */
    CalcReg implement(
        RexCall call,
        RexToCalcTranslator translator);

    boolean canImplement(RexCall call);
}

// End CalcRexImplementor.java
