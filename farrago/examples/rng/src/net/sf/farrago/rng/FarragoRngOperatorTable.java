/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import org.eigenbase.sql.fun.*;

/**
 * FarragoRngOperatorTable extends {@link SqlStdOperatorTable} with
 * the NEXT_RANDOM_INT operator provided by the RNG plugin.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRngOperatorTable extends SqlStdOperatorTable
{
    private static FarragoRngOperatorTable instance;

    public final FarragoRngNextRandomIntOperator nextRandomInt =
        new FarragoRngNextRandomIntOperator();

    /**
     * Retrieves the singleton, creating it if necessary.
     *
     * @return singleton with RNG-specific type
     */
    public static synchronized FarragoRngOperatorTable rngInstance()
    {
        if (instance == null) {
            instance = new FarragoRngOperatorTable();
            instance.init();
        }
        return instance;
    }

    /**
     * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} instance, creating it if necessary.
     *
     * @return singleton with generic type
     */
    public static SqlStdOperatorTable instance()
    {
        return rngInstance();
    }
}

// End FarragoRngOperatorTable.java
