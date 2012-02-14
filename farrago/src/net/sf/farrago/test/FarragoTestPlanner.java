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

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.hep.*;


/**
 * FarragoTestPlanner provides an implementation of the {@link
 * FarragoSessionPlanner} interface which allows for precise control over the
 * heuristic planner used during testing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTestPlanner
    extends HepPlanner
    implements FarragoSessionPlanner
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoPreparingStmt stmt;

    //~ Constructors -----------------------------------------------------------

    public FarragoTestPlanner(
        HepProgram program,
        FarragoPreparingStmt stmt)
    {
        super(program);
        this.stmt = stmt;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPlanner
    public FarragoSessionPreparingStmt getPreparingStmt()
    {
        return stmt;
    }

    // implement FarragoSessionPlanner
    public void beginMedPluginRegistration(String serverClassName)
    {
        // don't care
    }

    // implement FarragoSessionPlanner
    public void endMedPluginRegistration()
    {
        // don't care
    }

    // implement RelOptPlanner
    public JavaRelImplementor getJavaRelImplementor(RelNode rel)
    {
        return stmt.getRelImplementor(
            rel.getCluster().getRexBuilder());
    }
}

// End FarragoTestPlanner.java
