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
package org.eigenbase.sql2rel;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;


/**
 * Contains the context necessary for a {@link SqlRexConvertlet} to convert a
 * {@link SqlNode} expression into a {@link RexNode}.
 *
 * @author jhyde
 * @version $Id$
 * @since 2005/8/3
 */
public interface SqlRexContext
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param expr Expression to translate
     *
     * @return Converted expression
     */
    RexNode convertExpression(SqlNode expr);

    /**
     * Returns the {@link RexBuilder} to use to create {@link RexNode} objects.
     */
    RexBuilder getRexBuilder();

    /**
     * Returns the expression used to access a given IN or EXISTS {@link
     * SqlSelect sub-query}.
     *
     * @param call IN or EXISTS expression
     *
     * @return Expression used to access current row of sub-query
     */
    RexRangeRef getSubqueryExpr(SqlCall call);

    /**
     * Returns the type factory.
     */
    RelDataTypeFactory getTypeFactory();

    /**
     * Returns the factory which supplies default values for INSERT, UPDATE, and
     * NEW.
     */
    DefaultValueFactory getDefaultValueFactory();

    /**
     * Returns the validator.
     */
    SqlValidator getValidator();

    /**
     * Converts a literal.
     */
    RexNode convertLiteral(SqlLiteral literal);
}

// End SqlRexContext.java
