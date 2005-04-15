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

import net.sf.farrago.cwm.relational.enumerations.*;

/**
 * FarragoSessionDdlDropRule specifies the action to take when
 * an association link deletion event is detected during DDL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionDdlDropRule
{
    private final Class superInterface;
    
    private final ReferentialRuleTypeEnum action;
    
    private final String endName;

    /**
     * Creates a new FarragoSessionDdlDropRule object.
     *
     * @param endName the end to which this rule applies
     *
     * @param superInterface a filter on the instance of the end to which the
     * rule applies; if null, the rule applies to any object; otherwise, the
     * object must be an instance of this class
     *
     * @param action what to do when this rule fires
     */
    public FarragoSessionDdlDropRule(
        String endName,
        Class superInterface,
        ReferentialRuleTypeEnum action)
    {
        this.endName = endName;
        this.superInterface = superInterface;
        this.action = action;
    }

    public String getEndName()
    {
        return endName;
    }
    
    public Class getSuperInterface()
    {
        return superInterface;
    }

    public ReferentialRuleTypeEnum getAction()
    {
        return action;
    }
}

// End FarragoSessionDdlDropRule.java
