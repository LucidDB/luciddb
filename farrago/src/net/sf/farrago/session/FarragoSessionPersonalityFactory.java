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
package net.sf.farrago.session;

/**
 * FarragoSessionPersonalityFactory defines a factory interface for
 * creating instances of {@link FarragoSessionPersonality}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionPersonalityFactory
{
    /**
     * Creates a new session personality.
     *
     * @param defaultPersonality a default personality to which the
     * new personality may delegate, or null if no default is available
     *
     * @param session session for which personality is being created;
     * note that the personality may be used for other session as well,
     * so no reference to this session should be retained
     *
     * @return personality
     */
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality);
}

// End FarragoSessionPersonalityFactory.java
