/*
 * ColoredSourceWriter.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.debug.gui;


import java.io.*;
import java.util.Enumeration;
import openjava.ptree.*;
import openjava.mop.Environment;
import openjava.ptree.util.SourceCodeWriter;
import java.awt.*;
import javax.swing.*;
import javax.swing.text.*;


/**
 * The class <code>ColoredSourceWriter</code> is a Visitor role
 * in the Visitor pattern and this also visits each child
 * <code>ParseTree</code> object from left to right.
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.util.ParseTreeVisitor
 */
public class ColoredSourceWriter extends SourceCodeWriter
{
    public static SimpleAttributeSet regularAttr = new SimpleAttributeSet();
    static {
        StyleConstants.setFontFamily( regularAttr, "Courier" );
        StyleConstants.setFontSize( regularAttr, 12 );
    }
    SimpleAttributeSet calleeAttr;
    SimpleAttributeSet callerAttr;
    int callee_min;
    int caller_min;

    DefaultStyledDocument doc;
 
    class TextPanePrintWriter extends PrintWriter
    {
	public TextPanePrintWriter() {
	    super( System.err );
	}
	public void print( String s ) {
	    try {
		doc.insertString( doc.getLength(), s, lastAttr );
	    } catch ( BadLocationException e ) {
		System.err.println( e );
	    }
	}
	public void println( String s ) {
	    print( s + NEWLINE );
	}
	public void println() {
            print( NEWLINE );
        }
    }

    public ColoredSourceWriter( DefaultStyledDocument doc,
			        int callee_min, int caller_min )
    {
	super( null );
	out = new TextPanePrintWriter();
        this.doc = doc;
	this.callee_min = callee_min;
	this.caller_min = caller_min;
	lastAttr = regularAttr;
	calleeAttr = new SimpleAttributeSet( regularAttr );
	StyleConstants.setForeground( calleeAttr, Color.blue );
	callerAttr = new SimpleAttributeSet( regularAttr );
	StyleConstants.setForeground( callerAttr, Color.red );
    }

    SimpleAttributeSet lastAttr;
    SimpleAttributeSet beginColor( ParseTreeObject p ) {
	SimpleAttributeSet result = lastAttr;
	if (p.getObjectID() > callee_min) {
	    if (p.getObjectID() > caller_min) {
		lastAttr = callerAttr;
	    } else {
		lastAttr = calleeAttr;
	    }
	} else {
	    lastAttr = regularAttr;
	}
	return result;
    }
    void endColor( SimpleAttributeSet back ) {
	lastAttr = back;
    }

    public void visit( AllocationExpression p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ArrayAccess p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ArrayAllocationExpression p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ArrayInitializer p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( AssignmentExpression p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( BinaryExpression p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( Block p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( BreakStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( CaseGroup p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( CaseGroupList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( CaseLabel p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( CaseLabelList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( CastExpression p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( CatchBlock p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( CatchList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ClassDeclaration p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ClassDeclarationList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ClassLiteral p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( CompilationUnit p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ConditionalExpression p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ConstructorDeclaration p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ConstructorInvocation p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ContinueStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( DoWhileStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( EmptyStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ExpressionList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ExpressionStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( FieldAccess p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( FieldDeclaration p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ForStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( IfStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( InstanceofExpression p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( LabeledStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( Literal p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( MemberDeclarationList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( MemberInitializer p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( MethodCall p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( MethodDeclaration p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ModifierList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( Parameter p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ParameterList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ReturnStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( SelfAccess p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( StatementList p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( SwitchStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( SynchronizedStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( ThrowStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( TryStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( TypeName p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( UnaryExpression p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( Variable p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( VariableDeclaration p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( VariableDeclarator p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

    public void visit( WhileStatement p )
	throws ParseTreeException
    {
        SimpleAttributeSet back = beginColor( p );
        super.visit( p );
        endColor( back );
    }

}
