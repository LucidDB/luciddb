/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.session;

import net.sf.farrago.cwm.relational.enumerations.*;


/**
 * FarragoSessionDdlDropRule specifies the action to take when an association
 * link deletion event is detected during DDL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionDdlDropRule
{
    //~ Instance fields --------------------------------------------------------

    private final Class superInterface;

    private final ReferentialRuleTypeEnum action;

    private final String endName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoSessionDdlDropRule object.
     *
     * @param endName the end to which this rule applies
     * @param superInterface a filter on the instance of the end to which the
     * rule applies; if null, the rule applies to any object; otherwise, the
     * object must be an instance of this class
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

    //~ Methods ----------------------------------------------------------------

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
