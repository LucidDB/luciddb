/*
 * ScopeHandler.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.ptree.util;


import java.util.Stack;
import openjava.mop.*;
import openjava.ptree.*;


/**
 * Refinement of {@link EvaluationShuttle} which automatically pushes and
 * pops the current scope.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.util.ParseTreeVisitor
 * @see openjava.ptree.util.EvaluationShuttle
 */
public abstract class ScopeHandler extends EvaluationShuttle
{
    private Stack env_nest = new Stack();

    public ScopeHandler( Environment base_env ) {
        super( base_env );
    }

    protected final void pushClosedEnvironment() {
        env_nest.push( getEnvironment() );
        setEnvironment( new ClosedEnvironment( getEnvironment() ) );
    }

    protected final void push( Environment env ) {
        env_nest.push( getEnvironment() );
        setEnvironment( env );
    }

    protected final void pop() {
        setEnvironment( (Environment) env_nest.pop() );
    }

    /* in walking down through parse tree */

    /* compilation unit */
    public CompilationUnit evaluateDown(CompilationUnit ptree)
        throws ParseTreeException
    {
        ClassDeclaration pubclazz = ptree.getPublicClass();
        String name = (pubclazz != null) ? pubclazz.getName()
            : "<no public class>";
        FileEnvironment fenv
            = new FileEnvironment(getEnvironment(), ptree, name);

        push( fenv );

        return ptree;
    }

    /* class declaration */
    public ClassDeclaration evaluateDown( ClassDeclaration ptree )
        throws ParseTreeException
    {
        ClassEnvironment env
            = new ClassEnvironment( getEnvironment(), ptree.getName() );
        MemberDeclarationList mdecls = ptree.getBody();
        for (int i = 0; i < mdecls.size(); ++i) {
            MemberDeclaration m = mdecls.get( i );
            if (! (m instanceof ClassDeclaration))  continue;
            ClassDeclaration inner = (ClassDeclaration) m;
            env.recordMemberClass( inner.getName() );
        }
        push( env );
        return ptree;
    }

    /* class body contents */
    public MemberDeclaration evaluateDown( MethodDeclaration ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public MemberDeclaration evaluateDown( ConstructorDeclaration ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public MemberDeclaration evaluateDown( MemberInitializer ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    /* statements */
    public Statement evaluateDown( Block ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public Statement evaluateDown( SwitchStatement ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public Statement evaluateDown( IfStatement ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public Statement evaluateDown( WhileStatement ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public Statement evaluateDown( DoWhileStatement ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public Statement evaluateDown( ForStatement ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public Statement evaluateDown( TryStatement ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    public Statement evaluateDown( SynchronizedStatement ptree )
        throws ParseTreeException
    {
        pushClosedEnvironment();
        return ptree;
    }

    /* query */
    public openjava.ptree.Expression evaluateDown( QueryExpression ptree )
        throws ParseTreeException
    {
        QueryEnvironment env = new QueryEnvironment( getEnvironment(), ptree );
        push( env );
        return ptree;
    }

    /* in walking down through parse tree */

    /* class declaration */
    public CompilationUnit evaluateUp( CompilationUnit ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    /* class declaration */
    public ClassDeclaration evaluateUp( ClassDeclaration ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    /* class body contents */
    public MemberDeclaration evaluateUp( MethodDeclaration ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public MemberDeclaration evaluateUp( ConstructorDeclaration ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public MemberDeclaration evaluateUp( MemberInitializer ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    /* statements */
    public Statement evaluateUp( Block ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public Statement evaluateUp( SwitchStatement ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public Statement evaluateUp( IfStatement ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public Statement evaluateUp( WhileStatement ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public Statement evaluateUp( DoWhileStatement ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public Statement evaluateUp( ForStatement ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public Statement evaluateUp( TryStatement ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    public Statement evaluateUp( SynchronizedStatement ptree )
        throws ParseTreeException
    {
        pop();
        return ptree;
    }

    /* query */
    public Expression evaluateUp( QueryExpression ptree )
    {
        pop();
        return ptree;
    }
}

