/*
 * EvaluationShuttle.java
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


import openjava.ptree.*;
import openjava.mop.Environment;
import java.util.Vector;
import java.lang.reflect.Method;


/**
 * The class <code>EvaluationShuttle</code> is a Visitor role
 * in the Visitor pattern and this also visits each child
 * <code>ParseTree</code> object from left to right.
 * <p>
 * The class <code>Evaluator</code> is an evaluator of each
 * objects of <code>ParseTree</code> family.  Each methods in
 * this class is invoked from the class <code>EvaluationShuttle</code>.
 * <p>
 * The method <code>evaluateDown()</code> is invoked before evaluating
 * the children of the parse tree object, and <code>evaluateUp()</code>
 * is invoked after the evaluation.
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.util.ParseTreeVisitor
 */
public abstract class EvaluationShuttle extends ParseTreeVisitor
{
    private Environment env;

    public EvaluationShuttle( Environment env ) {
	this.env = env;
    }

    protected Environment getEnvironment() {
	return env;
    }

    protected void setEnvironment( Environment env ) {
	this.env = env;
    }

    public Expression evaluateDown( AllocationExpression p )
	throws ParseTreeException
    { return p; }
    public Expression evaluateDown( ArrayAccess p )
	throws ParseTreeException
    { return p; }
    public Expression evaluateDown( ArrayAllocationExpression p )
	throws ParseTreeException 
    { return p; }
    public VariableInitializer evaluateDown( ArrayInitializer p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( AssignmentExpression p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( BinaryExpression p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( Block p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( BreakStatement p )
	throws ParseTreeException 
    { return p; }
    public CaseGroup evaluateDown( CaseGroup p )
	throws ParseTreeException 
    { return p; }
    public CaseGroupList evaluateDown( CaseGroupList p )
	throws ParseTreeException 
    { return p; }
    public CaseLabel evaluateDown( CaseLabel p )
	throws ParseTreeException 
    { return p; }
    public CaseLabelList evaluateDown( CaseLabelList p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( CastExpression p )
	throws ParseTreeException 
    { return p; }
    public CatchBlock evaluateDown( CatchBlock p )
	throws ParseTreeException 
    { return p; }
    public CatchList evaluateDown( CatchList p )
	throws ParseTreeException 
    { return p; }
    public ClassDeclaration evaluateDown( ClassDeclaration p )
	throws ParseTreeException 
    { return p; }
    public ClassDeclarationList evaluateDown( ClassDeclarationList p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( ClassLiteral p )
	throws ParseTreeException 
    { return p; }
    public CompilationUnit evaluateDown( CompilationUnit p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( ConditionalExpression p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclaration evaluateDown( ConstructorDeclaration p )
	throws ParseTreeException 
    { return p; }
    public ConstructorInvocation evaluateDown( ConstructorInvocation p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( ContinueStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( DoWhileStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( EmptyStatement p )
	throws ParseTreeException 
    { return p; }
    public ExpressionList evaluateDown( ExpressionList p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( ExpressionStatement p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( FieldAccess p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclaration evaluateDown( FieldDeclaration p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( ForStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( IfStatement p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( InstanceofExpression p )
	throws ParseTreeException 
    { return p; }
    /*
    public JoinExpression evaluateDown( JoinExpression p )
	throws ParseTreeException 
    { return p; }
    */
    public Statement evaluateDown( LabeledStatement p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( Literal p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclarationList evaluateDown( MemberDeclarationList p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclaration evaluateDown( MemberInitializer p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( MethodCall p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclaration evaluateDown( MethodDeclaration p )
	throws ParseTreeException 
    { return p; }
    public ModifierList evaluateDown( ModifierList p )
	throws ParseTreeException 
    { return p; }
    public Parameter evaluateDown( Parameter p )
	throws ParseTreeException 
    { return p; }
    public ParameterList evaluateDown( ParameterList p )
	throws ParseTreeException 
    { return p; }
    /*
    public Expression evaluateDown( QueryExpression p )
	throws ParseTreeException 
    { return p; }
    */
    public Statement evaluateDown( ReturnStatement p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( SelfAccess p )
	throws ParseTreeException 
    { return p; }
    public StatementList evaluateDown( StatementList p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( SwitchStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( SynchronizedStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( ThrowStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( TryStatement p )
	throws ParseTreeException 
    { return p; }
    public TypeName evaluateDown( TypeName p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( UnaryExpression p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateDown( Variable p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( VariableDeclaration p )
	throws ParseTreeException 
    { return p; }
    public VariableDeclarator evaluateDown( VariableDeclarator p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateDown( WhileStatement p )
	throws ParseTreeException 
    { return p; }
    
    /*
    public Expression evaluateDown( AliasedExpression p )
	throws ParseTreeException 
    { return p; }
    */

    public Expression evaluateUp( AllocationExpression p )
	throws ParseTreeException
    { return p; }
    public Expression evaluateUp( ArrayAccess p )
	throws ParseTreeException
    { return p; }
    public Expression evaluateUp( ArrayAllocationExpression p )
	throws ParseTreeException 
    { return p; }
    public VariableInitializer evaluateUp( ArrayInitializer p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( AssignmentExpression p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( BinaryExpression p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( Block p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( BreakStatement p )
	throws ParseTreeException 
    { return p; }
    public CaseGroup evaluateUp( CaseGroup p )
	throws ParseTreeException 
    { return p; }
    public CaseGroupList evaluateUp( CaseGroupList p )
	throws ParseTreeException 
    { return p; }
    public CaseLabel evaluateUp( CaseLabel p )
	throws ParseTreeException 
    { return p; }
    public CaseLabelList evaluateUp( CaseLabelList p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( CastExpression p )
	throws ParseTreeException 
    { return p; }
    public CatchBlock evaluateUp( CatchBlock p )
	throws ParseTreeException 
    { return p; }
    public CatchList evaluateUp( CatchList p )
	throws ParseTreeException 
    { return p; }
    public ClassDeclaration evaluateUp( ClassDeclaration p )
	throws ParseTreeException 
    { return p; }
    public ClassDeclarationList evaluateUp( ClassDeclarationList p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( ClassLiteral p )
	throws ParseTreeException 
    { return p; }
    public CompilationUnit evaluateUp( CompilationUnit p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( ConditionalExpression p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclaration evaluateUp( ConstructorDeclaration p )
	throws ParseTreeException 
    { return p; }
    public ConstructorInvocation evaluateUp( ConstructorInvocation p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( ContinueStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( DoWhileStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( EmptyStatement p )
	throws ParseTreeException 
    { return p; }
    public ExpressionList evaluateUp( ExpressionList p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( ExpressionStatement p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( FieldAccess p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclaration evaluateUp( FieldDeclaration p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( ForStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( IfStatement p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( InstanceofExpression p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( LabeledStatement p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( Literal p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclarationList evaluateUp( MemberDeclarationList p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclaration evaluateUp( MemberInitializer p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( MethodCall p )
	throws ParseTreeException 
    { return p; }
    public MemberDeclaration evaluateUp( MethodDeclaration p )
	throws ParseTreeException 
    { return p; }
    public ModifierList evaluateUp( ModifierList p )
	throws ParseTreeException 
    { return p; }
    public Parameter evaluateUp( Parameter p )
	throws ParseTreeException 
    { return p; }
    public ParameterList evaluateUp( ParameterList p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( ReturnStatement p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( SelfAccess p )
	throws ParseTreeException 
    { return p; }
    public StatementList evaluateUp( StatementList p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( SwitchStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( SynchronizedStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( ThrowStatement p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( TryStatement p )
	throws ParseTreeException 
    { return p; }
    public TypeName evaluateUp( TypeName p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( UnaryExpression p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( Variable p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( VariableDeclaration p )
	throws ParseTreeException 
    { return p; }
    public VariableDeclarator evaluateUp( VariableDeclarator p )
	throws ParseTreeException 
    { return p; }
    public Statement evaluateUp( WhileStatement p )
	throws ParseTreeException 
    { return p; }
    /*
    public Expression evaluateUp( QueryExpression p )
	throws ParseTreeException 
    { return p; }
    public JoinExpression evaluateUp( JoinExpression p )
	throws ParseTreeException 
    { return p; }
    public Expression evaluateUp( AliasedExpression p )
	throws ParseTreeException 
    { return p; }
    */

    public void visit( AllocationExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ArrayAccess p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ArrayAllocationExpression p )
	throws ParseTreeException
    {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ArrayInitializer p ) throws ParseTreeException {
        VariableInitializer newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( AssignmentExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( BinaryExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( Block p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( BreakStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( CaseGroup p ) throws ParseTreeException {
	this.evaluateDown( p );
	p.childrenAccept( this );
	this.evaluateUp( p );
    }

    public void visit( CaseGroupList p ) throws ParseTreeException {
        CaseGroupList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( CaseLabel p ) throws ParseTreeException {
        CaseLabel newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( CaseLabelList p ) throws ParseTreeException {
        CaseLabelList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( CastExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( CatchBlock p ) throws ParseTreeException {
        CatchBlock newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( CatchList p ) throws ParseTreeException {
        CatchList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ClassDeclaration p ) throws ParseTreeException {
        ClassDeclaration newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ClassDeclarationList p ) throws ParseTreeException {
        ClassDeclarationList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ClassLiteral p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( CompilationUnit p ) throws ParseTreeException {
        CompilationUnit newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ConditionalExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ConstructorDeclaration p ) throws ParseTreeException {
        MemberDeclaration newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ConstructorInvocation p ) throws ParseTreeException {
        ConstructorInvocation newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ContinueStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( DoWhileStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( EmptyStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ExpressionList p ) throws ParseTreeException {
        ExpressionList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ExpressionStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    /* if not same as original, do not continue */
    public void visit( FieldAccess p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( FieldDeclaration p ) throws ParseTreeException {
        MemberDeclaration newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ForStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( IfStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( InstanceofExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( LabeledStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( Literal p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( MemberDeclarationList p ) throws ParseTreeException {
        MemberDeclarationList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( MemberInitializer p ) throws ParseTreeException {
        MemberDeclaration newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( MethodCall p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( MethodDeclaration p ) throws ParseTreeException {
        MemberDeclaration newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ModifierList p ) throws ParseTreeException {
        ModifierList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( Parameter p ) throws ParseTreeException {
        Parameter newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ParameterList p ) throws ParseTreeException {
        ParameterList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ReturnStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( SelfAccess p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( StatementList p ) throws ParseTreeException {
        StatementList newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( SwitchStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( SynchronizedStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( ThrowStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( TryStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( TypeName p ) throws ParseTreeException {
        TypeName newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( UnaryExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( Variable p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( VariableDeclaration p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( VariableDeclarator p ) throws ParseTreeException {
        VariableDeclarator newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( WhileStatement p ) throws ParseTreeException {
        Statement newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    /*
    public void visit( QueryExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( JoinExpression p ) throws ParseTreeException {
        JoinExpression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    public void visit( AliasedExpression p ) throws ParseTreeException {
        Expression newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    return;
	}
	p.childrenAccept( this );
	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }
    */
}

