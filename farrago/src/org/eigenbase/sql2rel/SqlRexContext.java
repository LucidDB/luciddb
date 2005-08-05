package org.eigenbase.sql2rel;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidator;

/**
 * Contains the context necessary for a {@link SqlRexConvertlet} to convert a
 * {@link SqlNode} expression into a {@link RexNode}.
 *
 * @author jhyde
 * @since 2005/8/3
 * @version $Id$
 */
public interface SqlRexContext
{
    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param expr Expression to translate
     * @return Converted expression
     */
    RexNode convertExpression(SqlNode expr);

    /**
     * Returns the {@link RexBuilder} to use to create {@link RexNode} objects.
     */
    RexBuilder getRexBuilder();

    /**
     * Returns the expression used to access a given IN or EXISTS
     * {@link SqlSelect sub-query}.
     *
     * @param call IN or EXISTS expression
     * @return Expression used to access current row of sub-query
     */
    RexRangeRef getSubqueryExpr(SqlCall call);

    /**
     * Returns the type factory.
     */
    RelDataTypeFactory getTypeFactory();

    /**
     * Returns the factory which supplies default values for INSERT, UPDATE,
     * and NEW.
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
