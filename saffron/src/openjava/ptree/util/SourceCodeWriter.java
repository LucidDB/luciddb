/*
 * SourceCodeWriter.java
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


import java.io.*;
import java.util.Enumeration;
import openjava.ptree.*;


/**
 * The class <code>SourceCodeWriter</code> is a Visitor role
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
public class SourceCodeWriter extends ParseTreeVisitor
{

    protected PrintWriter out;

    // Why this modifier is not final ?
    // Because of javac bug in excuting it with -O option.
    public static String NEWLINE;
    static {
        StringWriter strw = new StringWriter();
        PrintWriter pw = new PrintWriter( strw );
        pw.println();
        pw.close();
        NEWLINE = strw.toString();
    }

    /** to write debugging code */
    private int debugLevel = 0;

    /** to write debugging code */
    private String tab = "    ";
    private int nest = 0;

    /** to write debugging code */
    public void setDebugLevel( int n ) { debugLevel = n; }
    public int getDebugLevel() { return debugLevel; }
    public void setTab( String str ) { tab = str; }
    public String getTab() { return tab; }
    public void setNest( int i ) { nest = i; }
    public int getNest() { return nest; }
    public void pushNest() { setNest( getNest() + 1 ); }
    public void popNest() { setNest( getNest() - 1 ); }


    private final void writeDebugL( ParseTree ptree ) {
        if (getDebugLevel() > 0) {
            out.print( "[" );
            if (debugLevel > 1) {
                String qname = ptree.getClass().getName();
                String sname = qname.substring( qname.lastIndexOf( '.' ) + 1 );
                out.print( sname + "#" );
                if(debugLevel > 2){
                    out.print( ptree.getObjectID() );
                }
                out.print( " " );
            }
        }
    }

    private final void writeDebugR() {
        if (getDebugLevel() > 0) out.print( "]" );
    }

    private final void writeDebugLR() {
        if (getDebugLevel() > 0)  out.print( "[]" );
    }

    private final void writeDebugLln() {
        if (getDebugLevel() > 0)  out.println( "[" );
    }

    private final void writeDebugRln() {
        if (getDebugLevel() > 0)  out.println( "]" );
    }

    private final void writeDebugln() {
        if (getDebugLevel() > 0)  out.println();
    }

    private final void writeDebug( String str ) {
        if (getDebugLevel() > 0)  out.print( str );
    }

    private final void writeTab() {
        for (int i = 0; i < nest; i++)  out.print( getTab() );
    }

    /**
     * Allocates a source code writer.
     *
     */
    public SourceCodeWriter( PrintWriter out ) {
        super();
        this.out = out;
    }

    public void visit( AllocationExpression p )
        throws ParseTreeException
    {
        writeDebugL( p );

        Expression encloser = p.getEncloser();
        if (encloser != null) {
            encloser.accept( this );
            out.print( " . " );
        }

        out.print( "new " );

        TypeName tn = p.getClassType();
        tn.accept( this );

        ExpressionList args = p.getArguments();
        writeArguments( args );

        MemberDeclarationList mdlst = p.getClassBody();
        if (mdlst != null) {
            out.println( "{" );
            pushNest();  mdlst.accept( this );  popNest();
            writeTab();  out.print( "}" );
        }

        writeDebugR();
    }

    public void visit( ArrayAccess p )
        throws ParseTreeException
    {
        writeDebugL( p );

        Expression expr = p.getReferenceExpr();
        if (expr instanceof Leaf
            || expr instanceof ArrayAccess
            || expr instanceof FieldAccess
            || expr instanceof MethodCall
            || expr instanceof Variable) {
            expr.accept( this );
        } else {
            writeParenthesis( expr );
        }

        Expression index_expr = p.getIndexExpr();
        out.print( "[" );
        index_expr.accept( this );
        out.print( "]" );

        writeDebugR();
    }

    public void visit( ArrayAllocationExpression p )
        throws ParseTreeException
    {
        writeDebugL( p );

        out.print( "new " );

        TypeName tn = p.getTypeName();
        tn.accept( this );

        ExpressionList dl = p.getDimExprList();
        for (int i = 0; i < dl.size(); ++i) {
            Expression expr = dl.get( i );
            out.print( "[" );
            if (expr != null) {
                expr.accept( this );
            }
            out.print( "]" );
        }

        ArrayInitializer ainit = p.getInitializer();
        if (ainit != null)  ainit.accept( this );
        writeDebugR();
    }

    public void visit( ArrayInitializer p )
        throws ParseTreeException
    {
        writeDebugL( p );

        out.print( "{ " );
        writeListWithDelimiter( p, ", " );
        if (p.isRemainderOmitted())  out.print( "," );
        out.print( " }" );

        writeDebugR();
    }

    public void visit( AssignmentExpression p )
        throws ParseTreeException
    {
        writeDebugL( p );

        Expression lexpr = p.getLeft();
        if (lexpr instanceof AssignmentExpression) {
            writeParenthesis( lexpr );
        } else {
            lexpr.accept( this );
        }

        String operator = p.operatorString();
        out.print( " " + operator + " " );

        Expression rexp = p.getRight();
        rexp.accept( this );

        writeDebugR();
    }

    public void visit( BinaryExpression p )
        throws ParseTreeException
    {
        writeDebugL( p );

        Expression lexpr = p.getLeft();
        if (isOperatorNeededLeftPar( p.getOperator(), lexpr )) {
            writeParenthesis( lexpr );
        } else {
            lexpr.accept( this );
        }

        String operator = p.operatorString();
        out.print( " " + operator + " " );

        Expression rexpr = p.getRight();
        if (isOperatorNeededRightPar( p.getOperator(), rexpr )) {
            writeParenthesis( rexpr );
        } else {
            rexpr.accept( this );
        }

        writeDebugR();
    }

    public void visit( Block p )
        throws ParseTreeException
    {
        StatementList stmts = p.getStatements();
        writeTab();  writeDebugL( p );
        writeStatementsBlock( stmts );
        writeDebugR();  out.println();
    }

    public void visit( BreakStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "break" );

        String label = p.getLabel();
        if (label != null) {
            out.print( " " );
            out.print( label );
        }

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( CaseGroup p )
        throws ParseTreeException
    {
        ExpressionList labels = p.getLabels();
        for (int i = 0; i < labels.size(); ++i) {
            writeTab();
            Expression label = labels.get( i );
            if (label == null) {
                out.print( "default " );
            } else {
                out.print( "case " );
                label.accept( this );
            }
            out.println( " :" );
        }

        pushNest();
        StatementList stmts = p.getStatements();
        stmts.accept( this );
        popNest();
    }

    public void visit( CaseGroupList p )
        throws ParseTreeException
    {
        writeListWithSuffix( p, NEWLINE );
    }

    public void visit( CaseLabel p )
        throws ParseTreeException
    {
        Expression expr = p.getExpression();
        if (expr != null) {
            out.print( "case " );
            expr.accept( this );
        } else {
            out.print( "default" );
        }
        out.print( ":" );
    }

    public void visit( CaseLabelList p )
        throws ParseTreeException
    {
        writeListWithSuffix( p, NEWLINE );
    }

    public void visit( CastExpression p )
        throws ParseTreeException
    {
        writeDebugL( p );

        out.print( "(" );
        TypeName ts = p.getTypeSpecifier();
        ts.accept( this );
        out.print( ")" );

        out.print( " " );

        Expression expr = p.getExpression();
        if(expr instanceof AssignmentExpression
           || expr instanceof ConditionalExpression
           || expr instanceof BinaryExpression
           || expr instanceof InstanceofExpression
           || expr instanceof UnaryExpression){
            writeParenthesis( expr );
        } else {
            expr.accept( this );
        }

        writeDebugR();
    }

    public void visit( CatchBlock p )
        throws ParseTreeException
    {
        out.print( " catch " );

        out.print( "( " );

        Parameter param = p.getParameter();
        param.accept( this );

        out.print( " ) " );

        StatementList stmts = p.getBody();
        writeStatementsBlock( stmts );
    }

    public void visit( CatchList p )
        throws ParseTreeException
    {
        writeList( p );
    }

    public void visit( ClassDeclaration p )
        throws ParseTreeException
    {
        printComment( p );

        writeTab();  writeDebugL( p );

        /*ModifierList*/
        ModifierList modifs = p.getModifiers();
        modifs.accept( this );
        if (! modifs.isEmptyAsRegular())  out.print( " " );

        /*"class"*/
        if (p.isInterface()) {
            out.print( "interface " );
        } else {
            out.print( "class " );
        }

        String name = p.getName();
        out.print( name );

        /*"extends" TypeName*/
        TypeName[] zuper = p.getBaseclasses();
        if (zuper.length != 0){
            out.print( " extends " );
            zuper[0].accept( this );
            for (int i = 1; i < zuper.length; ++i) {
                out.print( ", " );
                zuper[i].accept( this );
            }
        } else {
            writeDebug( " " );  writeDebugLR();
        }

        /* "implements" ClassTypeList */
        TypeName[] impl = p.getInterfaces();
        if (impl.length != 0) {
            out.print( " implements " );
            impl[0].accept( this );
            for (int i = 1; i < impl.length; ++i) {
                out.print( ", " );
                impl[i].accept( this );
            }
        } else {
            writeDebug( " " );  writeDebugLR();
        }

        out.println();

        /* MemberDeclarationList */
        MemberDeclarationList classbody = p.getBody();
        writeTab();  out.println( "{" );
        if (classbody.isEmpty()) {
            classbody.accept( this );
        } else {
            out.println();
            pushNest();  classbody.accept( this );  popNest();
            out.println();
        }
        writeTab();  out.print( "}" );

        writeDebugR();  out.println();
    }

    public void visit( ClassDeclarationList p )
        throws ParseTreeException
    {
        writeListWithDelimiter( p, NEWLINE + NEWLINE );
    }

    public void visit( ClassLiteral p )
        throws ParseTreeException
    {
        writeDebugL( p );
        TypeName type = p.getTypeName();
        type.accept(this);
        out.print(".class");
        writeDebugR();
    }

    public void visit( CompilationUnit p )
        throws ParseTreeException
    {
        out.println( "/*" );
        out.println( " * This code was generated by ojc." );
        out.println( " */" );

        printComment( p );

        /* package statement */
        String qn = p.getPackage();
        if (qn != null) {
            writeDebugL( p );
            out.print( "package " + qn + ";" );
            writeDebugR(); out.println();

            out.println();  out.println();
        }

        /* import statement list */
        String[] islst = p.getDeclaredImports();
        if (islst.length != 0) {
            for (int i = 0; i < islst.length; ++i) {
                out.println( "import " + islst[i] + ";" );
            }
            out.println();  out.println();
        }

        /* type declaration list */
        ClassDeclarationList tdlst = p.getClassDeclarations();
        tdlst.accept( this );
    }

    public void visit( ConditionalExpression p )
        throws ParseTreeException
    {
        writeDebugL( p );

        Expression condition = p.getCondition();
        if (condition instanceof AssignmentExpression
            || condition instanceof ConditionalExpression) {
            writeParenthesis( condition );
        } else {
            condition.accept( this );
        }

        out.print( " ? " );

        Expression truecase = p.getTrueCase();
        if (truecase instanceof AssignmentExpression) {
            writeParenthesis( truecase );
        } else {
            truecase.accept( this );
        }

        out.print( " : " );

        Expression falsecase = p.getFalseCase();
        if (falsecase instanceof AssignmentExpression) {
            writeParenthesis( falsecase );
        } else {
            falsecase.accept( this );
        }

        writeDebugR();
    }

    public void visit( ConstructorDeclaration p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        ModifierList modifs = p.getModifiers();
        modifs.accept( this );
        if (! modifs.isEmptyAsRegular())  out.print( " " );

        String name = p.getName();
        out.print( name );

        ParameterList params = p.getParameters();
        out.print( "(" );
        if (params.size() != 0) {
            out.print( " " );
            params.accept( this );
            out.print( " " );
        }
        out.print( ")" );

        TypeName[] tnl = p.getThrows();
        if (tnl.length != 0) {
            out.println();  writeTab();  writeTab();
            out.print( "throws " );
            tnl[0].accept( this );
            for (int i = 1; i < tnl.length; ++i) {
                out.print ( ", " );
                tnl[i].accept( this );
            }
        }

        ConstructorInvocation sc = p.getConstructorInvocation();
        StatementList body = p.getBody();
        if (body == null && sc == null) {
            out.println( ";" );
        } else {
            out.println();

            writeTab();  out.println( "{" );
            pushNest();

            if (sc != null)  sc.accept( this );
            if (body != null)  body.accept( this );

            popNest();
            writeTab();  out.print( "}" );
        }

        writeDebugR();  out.println();
    }

    public void visit( ConstructorInvocation p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        if (p.isSelfInvocation()) {
            out.print( "this" );
        } else {
            Expression enclosing = p.getEnclosing();
            if (enclosing != null) {
                enclosing.accept( this );
                out.print( " . " );
            }
            out.print( "super" );
        }

        ExpressionList exprs = p.getArguments();
        writeArguments( exprs );

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( ContinueStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "continue" );

        String label = p.getLabel();
        if (label != null) {
            out.print( " " + label );
        }

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( DoWhileStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "do " );

        StatementList stmts = p.getStatements();

        if (stmts.isEmpty()) {
            out.print( " ; " );
        } else {
            writeStatementsBlock( stmts );
        }

        out.print( " while " );

        out.print( "(" );
        Expression expr = p.getExpression();
        expr.accept( this );
        out.print( ")" );

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( EmptyStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( ExpressionList p )
        throws ParseTreeException
    {
        writeListWithDelimiter( p, ", " );
    }

    public void visit( ExpressionStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        Expression expr = p.getExpression();
        expr.accept( this );

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( FieldAccess p )
        throws ParseTreeException
    {
        writeDebugL( p );

        Expression expr = p.getReferenceExpr();
        TypeName typename = p.getReferenceType();
        if (expr != null) {
            if (expr instanceof Leaf
                || expr instanceof ArrayAccess
                || expr instanceof FieldAccess
                || expr instanceof MethodCall
                || expr instanceof Variable) {
                expr.accept( this );
            } else {
                out.print( "(" );
                expr.accept( this );
                out.print( ")" );
            }
            out.print( "." );
        } else if (typename != null) {
            typename.accept( this );
            out.print( "." );
        }

        String name = p.getName();
        out.print( name );

        writeDebugR();
    }

    public void visit( FieldDeclaration p )
        throws ParseTreeException
    {
        printComment(p);

        writeTab();
        writeDebugL(p);

        /*ModifierList*/
        ModifierList modifs = p.getModifiers();
        modifs.accept(this);
        if (! modifs.isEmptyAsRegular())  out.print(" ");

        /*TypeName*/
        TypeName ts = p.getTypeSpecifier();
        ts.accept(this);

        out.print(" ");

        /*Variable*/
        String variable = p.getVariable();
        out.print(variable);

        /*"=" VariableInitializer*/
        VariableInitializer initializer = p.getInitializer();
        if (initializer != null) {
            out.print(" = ");
            initializer.accept(this);
        }
        /*";"*/
        out.print(";");

        writeDebugR();
        out.println();
    }

    public void visit( ForStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "for " );

        out.print( "(" );

        String varName = p.getVariable();
        if (varName != null) {
            // for (... in ...)
            Expression query = p.getQuery();
            out.print(varName);
            out.print(" in ");
            query.accept(this);

        } else {
            // for (...; ...; ...)

            ExpressionList init = p.getInit();
            TypeName tspec = p.getInitDeclType();
            VariableDeclarator[] vdecls = p.getInitDecls();
            if (init != null && (! init.isEmpty() )) {
                init.get( 0 ).accept( this );
                for (int i = 1; i < init.size(); ++i) {
                    out.print( ", " );
                    init.get( i ).accept( this );
                }
            } else if (tspec != null && vdecls != null && vdecls.length != 0) {
                tspec.accept( this );
                out.print( " " );
                vdecls[0].accept( this );
                for (int i = 1; i < vdecls.length; ++i) {
                    out.print( ", " );
                    vdecls[i].accept( this );
                }
            }

            out.print( ";" );

            Expression expr = p.getCondition();
            if (expr != null) {
                out.print( " " );
                expr.accept( this );
            }

            out.print( ";" );

            ExpressionList incr = p.getIncrement();
            if (incr != null && (! incr.isEmpty())) {
                out.print( " " );
                incr.get( 0 ).accept( this );
                for (int i = 1; i < incr.size(); ++i) {
                    out.print( ", " );
                    incr.get( i ).accept( this );
                }
            }
        }

        out.print( ") " );

        StatementList stmts = p.getStatements();
        if (stmts.isEmpty()) {
            out.print( ";" );
        } else {
            writeStatementsBlock( stmts );
        }

        writeDebugR();  out.println();
    }

    public void visit( IfStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "if " );

        out.print( "(" );
        Expression expr = p.getExpression();
        expr.accept( this );
        out.print( ") " );

        /* then part */
        StatementList stmts = p.getStatements();
        writeStatementsBlock( stmts );

        /* else part */
        StatementList elsestmts = p.getElseStatements();
        if (! elsestmts.isEmpty()) {
            out.print( " else " );
            writeStatementsBlock( elsestmts );
        }

        writeDebugR();  out.println();
    }

    public void visit( InstanceofExpression p )
        throws ParseTreeException
    {
        writeDebugL( p );

        /* this is too strict for + or - */
        Expression lexpr = p.getExpression();
        if (lexpr instanceof AssignmentExpression
            || lexpr instanceof ConditionalExpression
            || lexpr instanceof BinaryExpression) {
            writeParenthesis( lexpr );
        } else {
            lexpr.accept( this );
        }

        out.print( " instanceof " );

        TypeName tspec = p.getTypeSpecifier();
        tspec.accept( this );

        writeDebugR();
    }

    public void visit( LabeledStatement p )
        throws ParseTreeException
    {
        writeTab();

        String name = p.getLabel();
        out.print( name );

        out.println( " : " );

        Statement statement = p.getStatement();
        statement.accept( this );
    }

    public void visit( Literal p )
        throws ParseTreeException
    {
        out.print( p.toString() );
    }

    public void visit( MemberDeclarationList p )
        throws ParseTreeException
    {
        writeListWithDelimiter( p, NEWLINE );
    }

    public void visit( MemberInitializer p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        if (p.isStatic()) {
            out.print( "static " );
        }

        StatementList stmts = p.getBody();
        writeStatementsBlock( stmts );

        writeDebugR();
        out.println();
    }

    public void visit( MethodCall p )
        throws ParseTreeException
    {
        writeDebugL( p );

        Expression expr = p.getReferenceExpr();
        TypeName reftype = p.getReferenceType();
        if (expr != null) {
            if (expr instanceof Leaf
                || expr instanceof ArrayAccess
                || expr instanceof FieldAccess
                || expr instanceof MethodCall
                || expr instanceof Variable) {
                expr.accept( this );
            } else {
                writeParenthesis( expr );
            }
            out.print( "." );
        } else if (reftype != null) {
            reftype.accept( this );
            out.print( "." );
        }

        String name = p.getName();
        out.print( name );

        ExpressionList args = p.getArguments();
        writeArguments( args );

        writeDebugR();
    }

    public void visit( MethodDeclaration p )
        throws ParseTreeException
    {
        printComment( p );

        writeTab();  writeDebugL( p );

        ModifierList modifs = p.getModifiers();
        modifs.accept( this );
        if (! modifs.isEmptyAsRegular())  out.print( " " );

        TypeName ts = p.getReturnType();
        ts.accept( this );

        out.print( " " );

        String name = p.getName();
        out.print( name );

        ParameterList params = p.getParameters();
        out.print( "(" );
        if (! params.isEmpty()) {
            out.print( " " );  params.accept( this );  out.print( " " );
        } else {
            params.accept( this );
        }
        out.print( ")" );

        TypeName[] tnl = p.getThrows();
        if (tnl.length != 0) {
            out.println();  writeTab();  writeTab();
            out.print( "throws " );
            tnl[0].accept( this );
            for (int i = 1; i < tnl.length; ++i) {
                out.print ( ", " );
                tnl[i].accept( this );
            }
        }

        StatementList bl = p.getBody();
        if (bl == null) {
            out.print( ";" );
        } else {
            out.println();  writeTab();
            out.print( "{" );
            if (bl.isEmpty()) {
                bl.accept( this );
            } else {
                out.println();
                pushNest();  bl.accept( this );  popNest();
                writeTab();
            }
            out.print( "}" );
        }

        writeDebugR();  out.println();
    }

    public void visit( ModifierList p )
        throws ParseTreeException
    {
        writeDebugL( p );

        out.print( ModifierList.toString( p.getRegular() ) );

        writeDebugR();
    }

    public void visit( Parameter p )
        throws ParseTreeException
    {
        writeDebugL( p );

        ModifierList modifs = p.getModifiers();
        modifs.accept( this );
        if (! modifs.isEmptyAsRegular())  out.print( " " );

        TypeName typespec = p.getTypeSpecifier();
        typespec.accept( this );

        out.print( " " );

        String declname = p.getVariable();
        out.print( declname );

        writeDebugR();
    }

    public void visit( ParameterList p )
        throws ParseTreeException
    {
        writeListWithDelimiter( p, ", " );
    }

    public void visit( ReturnStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "return" );

        Expression expr = p.getExpression();
        if (expr != null) {
            out.print(" ");
            expr.accept( this );
        }

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( SelfAccess p )
        throws ParseTreeException
    {
        out.print( p.toString() );
    }

    public void visit( StatementList p )
        throws ParseTreeException
    {
        writeList( p );
    }

    public void visit( SwitchStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "switch " );
        out.print( "(" );
        Expression expr = p.getExpression();
        expr.accept( this );
        out.print( ")" );

        out.println( " {" );

        CaseGroupList casegrouplist = p.getCaseGroupList();
        casegrouplist.accept( this );

        writeTab();  out.print( "}" );  writeDebugR();  out.println();
    }

    public void visit( SynchronizedStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "synchronized " );

        out.print( "(" );
        Expression expr = p.getExpression();
        expr.accept( this );
        out.println( ")" );

        StatementList stmts = p.getStatements();
        writeStatementsBlock( stmts );

        writeDebugR();  out.println();
    }

    public void visit( ThrowStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "throw " );

        Expression expr = p.getExpression();
        expr.accept( this );

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( TryStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "try " );

        StatementList stmts = p.getBody();
        writeStatementsBlock( stmts );

        CatchList catchlist = p.getCatchList();
        if (! catchlist.isEmpty()) {
            catchlist.accept( this );
        }

        StatementList finstmts = p.getFinallyBody();
        if(! finstmts.isEmpty()){
            out.println( " finally " );
            writeStatementsBlock( finstmts );
        }

        writeDebugR();  out.println();
    }

    /******rough around innerclass********/
    public void visit( TypeName p )
        throws ParseTreeException
    {
        writeDebugL( p );

        String typename = p.getName().replace( '$', '.' );
        out.print( typename );

        int dims = p.getDimension();
        out.print( TypeName.stringFromDimension( dims ) );

        writeDebugR();
    }

    public void visit( UnaryExpression p )
        throws ParseTreeException
    {
        writeDebugL( p );

        if (p.isPrefix()) {
            String operator = p.operatorString();
            out.print( operator );
        }

        Expression expr = p.getExpression();
        if (expr instanceof AssignmentExpression
            || expr instanceof ConditionalExpression
            || expr instanceof BinaryExpression
            || expr instanceof InstanceofExpression
            || expr instanceof CastExpression
            || expr instanceof UnaryExpression){
            writeParenthesis( expr );
        } else {
            expr.accept( this );
        }

        if (p.isPostfix()) {
            String operator = p.operatorString();
            out.print( operator );
        }

        writeDebugR();
    }

    public void visit( Variable p )
        throws ParseTreeException
    {
        out.print( p.toString() );
    }

    public void visit( VariableDeclaration p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        ModifierList modifs = p.getModifiers();
        modifs.accept( this );
        if (! modifs.isEmptyAsRegular())  out.print( " " );

        TypeName typespec = p.getTypeSpecifier();
        typespec.accept( this );

        out.print( " " );

        VariableDeclarator vd = p.getVariableDeclarator();
        vd.accept( this );

        out.print( ";" );

        writeDebugR();  out.println();
    }

    public void visit( VariableDeclarator p )
        throws ParseTreeException
    {
        String declname = p.getVariable();
        out.print( declname );
        for (int i = 0; i < p.getDimension(); ++i) {
            out.print( "[]" );
        }

        VariableInitializer varinit = p.getInitializer();
        if (varinit != null) {
            out.print( " = " );
            varinit.accept( this );
        }
    }

    public void visit( WhileStatement p )
        throws ParseTreeException
    {
        writeTab();  writeDebugL( p );

        out.print( "while " );

        out.print( "(" );
        Expression expr = p.getExpression();
        expr.accept( this );
        out.print( ") " );

        StatementList stmts = p.getStatements();
        if (stmts.isEmpty()) {
            out.print( " ;" );
        } else {
            writeStatementsBlock( stmts );
        }

        writeDebugR();  out.println();
    }

    public void visit( QueryExpression p )
        throws ParseTreeException
    {
        out.print( "(select " );
        ExpressionList selectList = p.getSelectList();
        if (p.isBoxed()) {
            out.print("{");
            selectList.accept(this);
            out.print("}");
        } else {
            selectList.accept(this);
        }
        ExpressionList groupList = p.getGroupList();
        if (groupList != null) {
            out.write(" group by {");
            groupList.accept(this);
            out.write("}");
        }
        Expression from = p.getFrom();
        if (from != null) {
            out.write(" from ");
            from.accept(this);
        }
        Expression where = p.getWhere();
        if (where != null) {
            out.write(" where ");
            where.accept(this);
        }
        ExpressionList sort = p.getSort();
        if (sort != null) {
            out.write(" order by ");
            sort.accept(this);
        }
        out.print(")");
    }

    public void visit( AliasedExpression p )
        throws ParseTreeException
    {
        Expression child = p.getExpression();
        child.accept(this);
        String alias = p.getAlias();
        out.write(" as ");
        out.write(alias);
    }

    public void visit( JoinExpression p )
        throws ParseTreeException
    {
        Expression left = p.getLeft();
        left.accept(this);
        out.write(" " + p.getJoinTypeName() + " join ");
        Expression right = p.getRight();
        right.accept(this);
        Expression condition = p.getCondition();
        if (condition != null) {
            out.write(" on ");
            condition.accept(this);
        }
    }

    private final void writeArguments( ExpressionList args )
        throws ParseTreeException
    {
        out.print( "(" );
        if (! args.isEmpty()) {
            out.print( " " );
            args.accept( this );
            out.print( " " );
        } else {
            args.accept( this );
        }
        out.print( ")" );
    }

    private final void writeAnonymous( Object obj )
        throws ParseTreeException
    {
        if (obj == null) {
            writeDebug( "#null" );
        } else if (obj instanceof ParseTree) {
            ((ParseTree) obj).accept( this );
        } else {
            out.print( obj.toString() );
        }
    }

    private final void writeList( List list )
        throws ParseTreeException
    {
        Enumeration it = list.elements();

        while (it.hasMoreElements()) {
            Object elem = it.nextElement();
            writeAnonymous( elem );
        }
    }

    private final void writeListWithDelimiter( List list, String delimiter )
        throws ParseTreeException
    {
        Enumeration it = list.elements();

        if (! it.hasMoreElements())  return;

        writeAnonymous( it.nextElement() );
        while (it.hasMoreElements()) {
            out.print( delimiter );
            writeAnonymous( it.nextElement() );
        }
    }

    private final void writeListWithSuffix( List list, String suffix )
        throws ParseTreeException
    {
        Enumeration it = list.elements();

        while (it.hasMoreElements()) {
            writeAnonymous( it.nextElement() );
            out.print( suffix );
        }
    }

    private final void writeParenthesis( Expression expr )
        throws ParseTreeException
    {
        out.print( "(" );
        expr.accept( this );
        out.print( ")" );
    }

    private final void writeStatementsBlock( StatementList stmts )
        throws ParseTreeException
    {
        out.println( "{" );  pushNest();

        stmts.accept( this );

        popNest();  writeTab();  out.print( "}" );
    }

    private static final boolean
    isOperatorNeededLeftPar( int operator, Expression leftexpr ) {
        if (leftexpr instanceof AssignmentExpression
           || leftexpr instanceof ConditionalExpression) {
            return true;
        }

        int op = operatorStrength( operator );

        if (leftexpr instanceof InstanceofExpression) {
            return (op > operatorStrength( BinaryExpression.INSTANCEOF ));
        }

        if(! (leftexpr instanceof BinaryExpression))  return false;

        BinaryExpression lbexpr = (BinaryExpression) leftexpr;
        return (op > operatorStrength( lbexpr.getOperator() ));
    }

    private static final boolean
    isOperatorNeededRightPar( int operator, Expression rightexpr ) {
        if (rightexpr instanceof AssignmentExpression
           || rightexpr instanceof ConditionalExpression) {
            return true;
        }

        int op = operatorStrength( operator );

        if (rightexpr instanceof InstanceofExpression) {
            return (op >= operatorStrength( BinaryExpression.INSTANCEOF ));
        }

        if (! (rightexpr instanceof BinaryExpression))  return false;

        BinaryExpression lbexpr = (BinaryExpression) rightexpr;
        return (op >= operatorStrength( lbexpr.getOperator() ));
    }

    /**
     * Returns the strength of the union of the operator.
     *
     * @param  op  the id number of operator.
     * @return  the strength of the union.
     */
    private static final int operatorStrength( int op ) {
        switch (op) {
        case BinaryExpression.TIMES :
        case BinaryExpression.DIVIDE :
        case BinaryExpression.MOD :
            return 40;
        case BinaryExpression.PLUS :
        case BinaryExpression.MINUS :
            return 35;
        case BinaryExpression.SHIFT_L :
        case BinaryExpression.SHIFT_R :
        case BinaryExpression.SHIFT_RR :
            return 30;
        case BinaryExpression.LESS :
        case BinaryExpression.GREATER :
        case BinaryExpression.LESSEQUAL :
        case BinaryExpression.GREATEREQUAL :
        case BinaryExpression.INSTANCEOF :
            return 25;
        case BinaryExpression.EQUAL :
        case BinaryExpression.NOTEQUAL :
            return 20;
        case BinaryExpression.BITAND :
            return 16;
        case BinaryExpression.XOR :
            return 14;
        case BinaryExpression.BITOR :
            return 12;
        case BinaryExpression.LOGICAL_AND :
            return 10;
        case BinaryExpression.LOGICAL_OR :
            return 8;
        }
        return 100;
    }

    private final void printComment( NonLeaf p ) {
        String comment = p.getComment();
        if (comment != null) {
            writeTab();
            out.println( comment );
        }
    }

}
