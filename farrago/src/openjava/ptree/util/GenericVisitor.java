/*
 * GenericVisitor.java
 *
 * @author   Julian Hyde
 * @version  %VERSION% %DATE%
 */
package openjava.ptree.util;


import openjava.ptree.*;
import openjava.mop.Environment;


/**
 * The class <code>GenericVisitor</code> is a Visitor role in the Visitor
 * pattern and visits <code>ParseTree</code> objects as the role of Element.
 * All of the <code>visit</code> methods of the base class, {@link
 * ParseTreeVisitor}, redirect to a the {@link #visit(ParseTree)} method.  A
 * derived class can override this method to treat all nodes uniformly.
 *
 * @author   Julian Hyde
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see openjava.ptree.ParseTree
 **/
public abstract class GenericVisitor extends ParseTreeVisitor
{
    /**
     * Processes a <code>ParseTree</code> node.  All other <code>visit</code>
     * methods call this one.
     */
    public void visitGeneric( ParseTree p ) throws ParseTreeException
    {
	p.childrenAccept(this);
    }

    public void visit( ParseTreeObject p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( NonLeaf p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( Leaf p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( MemberDeclaration p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( Statement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( Expression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( VariableInitializer p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( List p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( AllocationExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ArrayAccess p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ArrayAllocationExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ArrayInitializer p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( AssignmentExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( BinaryExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( Block p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( BreakStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( CaseGroup p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( CaseGroupList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( CaseLabel p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( CaseLabelList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( CastExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( CatchBlock p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( CatchList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ClassDeclaration p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ClassDeclarationList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ClassLiteral p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( CompilationUnit p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ConditionalExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ConstructorDeclaration p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ConstructorInvocation p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ContinueStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( DoWhileStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( EmptyStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ExpressionList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ExpressionStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( FieldAccess p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( FieldDeclaration p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ForStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( IfStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( InstanceofExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( LabeledStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( Literal p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( MemberDeclarationList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( MemberInitializer p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( MethodCall p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( MethodDeclaration p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ModifierList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( Parameter p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ParameterList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ReturnStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( SelfAccess p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( StatementList p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( SwitchStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( SynchronizedStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( ThrowStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( TryStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( TypeName p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( UnaryExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( Variable p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( VariableDeclaration p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( VariableDeclarator p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( WhileStatement p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    /*
    public void visit( QueryExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( AliasedExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    public void visit( JoinExpression p )
	throws ParseTreeException
    {
	visitGeneric( p );
    }
    */
}
