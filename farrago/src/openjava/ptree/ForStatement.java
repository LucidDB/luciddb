/*
 * ForStatement.java 1.0
 *
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 10, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 10, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;


/**
 * The <code>ForStatement</code> class represents a for statement
 * node of parse tree.
 * <br>
 * The specification of the initialization part may be modified
 * in the later version of OpenJava.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 * @see openjava.ptree.Expression
 * @see openjava.ptree.ExpressionList
 * @see openjava.ptree.StatementList
 */
public class ForStatement extends NonLeaf
    implements Statement, ParseTree
{

    /**
     * Allocates a new ForStatement object.
     *
     */
    ForStatement() {
        super();
    }

    /**
     * Allocates a new ForStatement object.
     *
     * @param  init  the initialization part.
     * @param  expr  the condition part.
     * @param  iterator  the increments part.
     * @param  stmts  the stement list of the body.
     */
    public ForStatement(
        ExpressionList  init,
        Expression      expr,
        ExpressionList  iterator,
        StatementList   stmts )
    {
        super();
        if (iterator == null)  iterator = new ExpressionList();
        if (stmts == null)  stmts = new StatementList();
        set( null, null, init, expr, iterator, stmts, null, null );
    }

    public ForStatement(
        TypeName                tspec,
        VariableDeclarator[]    vdecls,
        Expression              expr,
        ExpressionList          iterator,
        StatementList   stmts )
    {
        super();
        if (stmts == null)  stmts = new StatementList();
        if (tspec == null || vdecls == null || vdecls.length == 0) {
            set( null, null, null, expr, iterator, stmts, null, null );
        } else {
            set( tspec, vdecls, null, expr, iterator, stmts, null, null );
        }
    }

    /**
     * Allocates a new ForStatement object of the form <code>for (variable in
     * query) &hellip;</code>.
     *
     * @param  varName  name of variable
     * @param  query  query expression
     * @param  stmts  the statement list of the body
     */
    public ForStatement(
        String varName,
        Expression query,
        StatementList stmts)
    {
        super();
        if (stmts == null) stmts = new StatementList();
        set( null, null, null, null, null, stmts, varName, query );
    }

    public void writeCode() {
        writeTab();  writeDebugL();

        out.print( "for" );
        out.print( " (" );

        String varName = getVariable();
        if (varName != null) {
            // for (... in ...)
            Expression query = getQuery();
            out.print(varName);
            out.print(" in ");
            query.writeCode();

        } else {
            // for (...; ...; ...)
            ExpressionList init = getInit();
            TypeName tspec = getInitDeclType();
            VariableDeclarator[] vdecls = getInitDecls();
            if (init != null && (! init.isEmpty() )) {
                init.get( 0 ).writeCode();
                for (int i = 1; i < init.size(); ++i) {
                    out.print( ", " );
                    init.get( i ).writeCode();
                }
            } else if (tspec != null && vdecls != null && vdecls.length != 0) {
                tspec.writeCode();
                out.print( " " );
                vdecls[0].writeCode();
                for (int i = 1; i < vdecls.length; ++i) {
                    out.print( ", " );
                    vdecls[i].writeCode();
                }
            }

            out.print( ";" );

            Expression expr = getCondition();
            if (expr != null) {
                out.print( " " );
                expr.writeCode();
            }

            out.print( ";" );

            ExpressionList incr = getIncrement();
            if (incr != null && (! incr.isEmpty())) {
                out.print( " " );
                incr.get( 0 ).writeCode();
                for (int i = 1; i < incr.size(); ++i) {
                    out.print( ", " );
                    incr.get( i ).writeCode();
                }
            }
        }

        out.print( ")" );

        StatementList stmts = getStatements();
        if (stmts.isEmpty()) {
            out.print( " ;" );
        } else {
            out.println( " {" );
            pushNest();  stmts.writeCode();  popNest();
            writeTab();  out.print( "}" );
        }

        writeDebugR();  out.println();
    }

    /**
     * Gets the initialization part of this for-statement.
     *
     * @return  the initialization part.
     */
    public ExpressionList getInit() {
        return (ExpressionList) elementAt( 2 );
    }

    /**
     * Sets the initialization part of this for-statement.
     *
     * @param  init  the initialization part.
     */
    public void setInit( ExpressionList init ) {
        setElementAt( null, 0 );
        setElementAt( null, 1 );
        setElementAt( init, 2 );
    }

    /**
     * Gets the initialization part of this for-statement.
     *
     * @return  the initialization part.
     */
    public TypeName getInitDeclType() {
        return (TypeName) elementAt( 0 );
    }

    /**
     * Gets the initialization part of this for-statement.
     *
     * @return  the initialization part.
     */
    public VariableDeclarator[] getInitDecls() {
        return (VariableDeclarator[]) elementAt( 1 );
    }

    /**
     * Sets the initialization part of this for-statement.
     *
     * @param  init  the initialization part.
     */
    public void setInitDecl( TypeName type_specifier,
                             VariableDeclarator[] init ) {
        if (type_specifier == null || init == null || init.length == 0) {
            setElementAt( null, 0 );
            setElementAt( null, 1 );
        } else {
            setElementAt( type_specifier, 0 );
            setElementAt( init, 1 );
        }
        setElementAt( null, 2 );
    }

    /**
     * Gets the condition part of this for-statement.
     *
     * @return  the expression of the condtion part.
     */
    public Expression getCondition() {
        return (Expression) elementAt( 3 );
    }

    /**
     * Sets the condition part of this for-statement.
     *
     * @param  cond  the expression of the condtion part.
     */
    public void setCondition( Expression cond ) {
        setElementAt( cond, 3 );
    }

    /**
     * Gets the increments part of this for-statement.
     *
     * @return  the expression list of the increments part.
     */
    public ExpressionList getIncrement() {
        return (ExpressionList) elementAt( 4 );
    }

    /**
     * Sets the increments part of this for-statement.
     *
     * @param  incs  the expression list of the increments part.
     */
    public void setIncrement( ExpressionList incs ) {
        if (incs == null)  incs = new ExpressionList();
        setElementAt( incs, 4 );
    }

    /**
     * Gets the body of this for-statement.
     *
     * @return  the statement list of the body.
     */
    public StatementList getStatements() {
        return (StatementList) elementAt( 5 );
    }

    /**
     * Sets the body of this for-statement.
     *
     * @param  stmts  the statement list of the body.
     */
    public void setStatements( StatementList stmts ) {
        if (stmts == null)  stmts = new StatementList();
        setElementAt( stmts, 5 );
    }

    /**
     * Gets the name of the 'in' variable in a for-statement.
     *
     * @return  the name of the variable.
     */
    public String getVariable() {
        return (String) elementAt( 6 );
    }

    /**
     * Sets the name of the 'in' variable of this for-statement.
     *
     * @param  stmts  the name of the 'in' variable.
     */
    public void setVariable( String name ) {
        setElementAt( name, 6 );
    }

    /**
     * Gets the 'in' expression of this for-statement.
     *
     * @return  the 'in' expression.
     */
    public Expression getQuery() {
        return (Expression) elementAt( 7 );
    }

    /**
     * Sets the 'in' expression of this for-statement.
     *
     * @param  query  the 'in' expression.
     */
    public void setQuery( Expression query ) {
        setElementAt( query, 7 );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
