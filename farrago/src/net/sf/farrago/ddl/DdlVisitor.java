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
package net.sf.farrago.ddl;

/**
 * DdlVisitor implements the visitor pattern for DDL statements.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlVisitor
{
    //~ Methods ----------------------------------------------------------------

    // visitor dispatch
    public void visit(DdlCreateStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlDropStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlAlterStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlCommitStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlRollbackStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSavepointStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlReleaseSavepointStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetCatalogStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetRoleStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetSchemaStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetPathStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetParamStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetSystemParamStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetSessionParamStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlCheckpointStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlExtendCatalogStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlReplaceCatalogStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetSessionImplementationStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlGrantStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlImportForeignSchemaStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlDeallocateOldStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlMultipleTransactionStmt stmt)
    {
    }
}

// End DdlVisitor.java
