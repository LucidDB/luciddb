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
package net.sf.farrago.test;

import java.util.*;

import net.sf.farrago.db.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.sql.*;


/**
 * FarragoTestPersonalityFactory implements the {@link
 * FarragoSessionPersonalityFactory} interface with some tweaks just for
 * testing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTestPersonalityFactory
    implements FarragoSessionPersonalityFactory
{
    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPersonalityFactory
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        return new FarragoTestSessionPersonality((FarragoDbSession) session);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class FarragoTestSessionPersonality
        extends FarragoDefaultSessionPersonality
    {
        protected FarragoTestSessionPersonality(FarragoDbSession session)
        {
            super(session);
        }

        // implement FarragoSessionPersonality
        public void registerRelMetadataProviders(
            ChainedRelMetadataProvider chain)
        {
            chain.addProvider(new FarragoTestRelMetadataProvider());
        }

        // implement FarragoSessionPersonality
        public FarragoSessionPlanner newPlanner(
            FarragoSessionPreparingStmt stmt,
            boolean init)
        {
            // NOTE jvs 17-Nov-2008:  This is a hack to trigger
            // a badly-behaving planner for testing planner abort.
            if ("BAD_VOLCANO".equals(
                    stmt.getSession().getSessionVariables().schemaName))
            {
                FarragoDefaultPlanner planner = new FarragoDefaultPlanner(stmt);
                if (init) {
                    planner.init();
                    planner.addRule(
                        PullUpProjectsAboveJoinRule.instanceTwoProjectChildren);
                }
                return planner;
            } else {
                return super.newPlanner(stmt, init);
            }
        }
    }

    public static class FarragoTestRelMetadataProvider
        extends ReflectiveRelMetadataProvider
    {
        FarragoTestRelMetadataProvider()
        {
            mapParameterTypes(
                "isVisibleInExplain",
                Collections.singletonList((Class) SqlExplainLevel.class));
        }

        public Double getRowCount(AggregateRelBase rel)
        {
            // Lie and say aggregates always returns a million rows.
            return 1000000.0;
        }

        public Boolean isVisibleInExplain(
            FennelToIteratorConverter rel,
            SqlExplainLevel level)
        {
            // Hide instances of FennelToIteratorConverter from EXPLAIN PLAN
            // unless WITH ALL ATTRIBUTES is specified.
            return level == SqlExplainLevel.ALL_ATTRIBUTES;
        }
    }
}

// End FarragoTestPersonalityFactory.java
