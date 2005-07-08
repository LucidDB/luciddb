/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.runtime;

import net.sf.farrago.session.FarragoSessionRuntimeParams;
import org.eigenbase.runtime.RestartableIterator;

/**
 * Sandbox for experiments in code generation.
 *
 * <p>THIS IS NOT A TEST! (Well not as such.)
 *
 * <p>This code fragments in this class serves two purposes: <ul>
 * <li>As an example of the code produced by a code-generator
 *    (cross-referenced by javadoc comments in the code-generator); and</li>
 * <li>To help analyse the impact of changing runtime data structures. Suppose
 *     we were to rename the
 *     {@link FarragoRuntimeContext#newJavaTupleStream} method. Then
 *     the {@link #exampleJavaTupleStream()} method would fail to compile. We
 *     can find the generator this code by finding all javadoc references to
 *     the {@link #exampleJavaTupleStream()} method.
 * </ul>
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FarragoGeneratedCodeExamples
{
    public void exampleJavaTupleStream()
    {
        FarragoSessionRuntimeParams params = null;
        FarragoRuntimeContext cx = new FarragoRuntimeContext(params);

        int streamId = 0;
        FennelTupleWriter tupleWriter = null;
        RestartableIterator iter = null;
        cx.newJavaTupleStream(streamId, tupleWriter, iter);
    }
}

// End FarragoGeneratedCodeExamples.java
