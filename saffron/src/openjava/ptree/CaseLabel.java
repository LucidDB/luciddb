/*
 * CaseLabel.java 1.0
 *
 * Jun 20, 1997 mich
 * Sep 29, 1997 bv
 * Oct 11, 1997 mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 11, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The CaseLabel class presents for the ptree-node like
 * "case 1 :"
 *
 */
public class CaseLabel extends NonLeaf
{
    public CaseLabel(Expression expr) {
	super();
	set( (ParseTree) expr );
    }

    CaseLabel() {
	super();
    }
    
    public void writeCode() {
	Expression expr = getExpression();
	if (expr != null) {
	    out.print("case ");
	    expr.writeCode();
	} else{
	    out.print("default");
	}
	out.print(":");
    }
  
    public Expression getExpression() {
	return (Expression) elementAt(0);
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

}
