/*
 * ParseTreeVisitor.java
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

/**
 * The class <code>ParseTreeVisitor</code> is a Visitor role
 * in the Visitor pattern and visits <code>ParseTree</code> objects
 * as the role of Element.
 * <p>
 * For example
 * <pre>
 * </pre>
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see openjava.ptree.ParseTree
 */
public abstract class ParseTreeVisitor
{

    public void visit( ParseTree p )
	throws ParseTreeException
    {
	p.accept( this );
    }
    public void visit( ParseTreeObject p )
	throws ParseTreeException
    {
	p.accept( this );
    }
    public void visit( NonLeaf p )
	throws ParseTreeException
    {
	p.accept( this );
    }
    public void visit( Leaf p )
	throws ParseTreeException
    {
	p.accept( this );
    }
    public void visit( MemberDeclaration p )
	throws ParseTreeException
    {
	p.accept( this );
    }
    public void visit( Statement p )
	throws ParseTreeException
    {
	p.accept( this );
    }
    public void visit( Expression p )
	throws ParseTreeException
    {
	p.accept( this );
    }
    public void visit( VariableInitializer p )
	throws ParseTreeException
    {
	p.accept( this );
    }
    public void visit( List p )
	throws ParseTreeException
    {
	p.accept( this );
    }

    public abstract void visit( AllocationExpression p )
	throws ParseTreeException;
    public abstract void visit( ArrayAccess p )
	throws ParseTreeException;
    public abstract void visit( ArrayAllocationExpression p )
	throws ParseTreeException;
    public abstract void visit( ArrayInitializer p )
	throws ParseTreeException;
    public abstract void visit( AssignmentExpression p )
	throws ParseTreeException;
    public abstract void visit( BinaryExpression p )
	throws ParseTreeException;
    public abstract void visit( Block p )
	throws ParseTreeException;
    public abstract void visit( BreakStatement p )
	throws ParseTreeException;
    public abstract void visit( CaseGroup p )
	throws ParseTreeException;
    public abstract void visit( CaseGroupList p )
	throws ParseTreeException;
    public abstract void visit( CaseLabel p )
	throws ParseTreeException;
    public abstract void visit( CaseLabelList p )
	throws ParseTreeException;
    public abstract void visit( CastExpression p )
	throws ParseTreeException;
    public abstract void visit( CatchBlock p )
	throws ParseTreeException;
    public abstract void visit( CatchList p )
	throws ParseTreeException;
    public abstract void visit( ClassDeclaration p )
	throws ParseTreeException;
    public abstract void visit( ClassDeclarationList p )
	throws ParseTreeException;
    public abstract void visit( ClassLiteral p )
	throws ParseTreeException;
    public abstract void visit( CompilationUnit p )
	throws ParseTreeException;
    public abstract void visit( ConditionalExpression p )
	throws ParseTreeException;
    public abstract void visit( ConstructorDeclaration p )
	throws ParseTreeException;
    public abstract void visit( ConstructorInvocation p )
	throws ParseTreeException;
    public abstract void visit( ContinueStatement p )
	throws ParseTreeException;
    public abstract void visit( DoWhileStatement p )
	throws ParseTreeException;
    public abstract void visit( EmptyStatement p )
	throws ParseTreeException;
    public abstract void visit( ExpressionList p )
	throws ParseTreeException;
    public abstract void visit( ExpressionStatement p )
	throws ParseTreeException;
    public abstract void visit( FieldAccess p )
	throws ParseTreeException;
    public abstract void visit( FieldDeclaration p )
	throws ParseTreeException;
    public abstract void visit( ForStatement p )
	throws ParseTreeException;
    public abstract void visit( IfStatement p )
	throws ParseTreeException;
    public abstract void visit( InstanceofExpression p )
	throws ParseTreeException;
    public abstract void visit( LabeledStatement p )
	throws ParseTreeException;
    public abstract void visit( Literal p )
	throws ParseTreeException;
    public abstract void visit( MemberDeclarationList p )
	throws ParseTreeException;
    public abstract void visit( MemberInitializer p )
	throws ParseTreeException;
    public abstract void visit( MethodCall p )
	throws ParseTreeException;
    public abstract void visit( MethodDeclaration p )
	throws ParseTreeException;
    public abstract void visit( ModifierList p )
	throws ParseTreeException;
    public abstract void visit( Parameter p )
	throws ParseTreeException;
    public abstract void visit( ParameterList p )
	throws ParseTreeException;
    public abstract void visit( ReturnStatement p )
	throws ParseTreeException;
    public abstract void visit( SelfAccess p )
	throws ParseTreeException;
    public abstract void visit( StatementList p )
	throws ParseTreeException;
    public abstract void visit( SwitchStatement p )
	throws ParseTreeException;
    public abstract void visit( SynchronizedStatement p )
	throws ParseTreeException;
    public abstract void visit( ThrowStatement p )
	throws ParseTreeException;
    public abstract void visit( TryStatement p )
	throws ParseTreeException;
    public abstract void visit( TypeName p )
	throws ParseTreeException;
    public abstract void visit( UnaryExpression p )
	throws ParseTreeException;
    public abstract void visit( Variable p )
	throws ParseTreeException;
    public abstract void visit( VariableDeclaration p )
	throws ParseTreeException;
    public abstract void visit( VariableDeclarator p )
	throws ParseTreeException;
    public abstract void visit( WhileStatement p )
	throws ParseTreeException;
    /*
    public abstract void visit( QueryExpression p )
	throws ParseTreeException;
    public abstract void visit( AliasedExpression p )
	throws ParseTreeException;
    public abstract void visit( JoinExpression p )
	throws ParseTreeException;
    */
}
