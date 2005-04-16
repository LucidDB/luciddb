/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.rng;

import net.sf.farrago.ojrex.*;

import java.lang.reflect.*;

import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;

/**
 * FarragoRngImplementorTable extends {@link FarragoOJRexImplementorTable}
 * with code generation for the NEXT_RANDOM_INT operator.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRngImplementorTable extends FarragoOJRexImplementorTable
{
    private static FarragoRngImplementorTable instance;

    private FarragoRngImplementorTable(SqlStdOperatorTable opTab)
    {
        super(opTab);

        Method method;

        try {
            method = FarragoRngUDR.class.getMethod(
                "rng_next_int_internal",
                new Class [] {
                    Integer.TYPE,
                    String.class,
                    String.class
                });
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
        registerOperator(
            FarragoRngOperatorTable.rngInstance().nextRandomInt,
            new FarragoOJRexStaticMethodImplementor(method, false, null));
    }

    /**
     * Retrieves the singleton, creating it if necessary.
     *
     * @return singleton with RNG-specific type
     */
    public static synchronized FarragoRngImplementorTable rngInstance()
    {
        if (instance == null) {
            instance = new FarragoRngImplementorTable(
                FarragoRngOperatorTable.rngInstance());
        }
        return instance;
    }
}

// End FarragoRngImplementorTable.java
