/*
 * MemberDeclaration.java 1.0
 *
 * This interface is made to type ptree-node into the field
 * declaration in the body of class.
 *
 * Jun 20, 1997
 * Aug 20, 1997
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Aug 20, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The MemberDeclaration interface types ptree-node into the member
 * declaration in the body of class.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.FieldDeclaration
 * @see openjava.ptree.MethodDeclaration
 * @see openjava.ptree.ConstructorDeclaration
 * @see openjava.ptree.MemberInitializer
 * @see openjava.ptree.ClassDeclaration
 */
public interface MemberDeclaration
    extends ParseTree
{
  /** The FIELD is a kind of MemberDeclaration */
  public static final int FIELD = 48;

  /** The METHOD is a kind of MemberDeclaration */
  public static final int METHOD = 49;

  /** The CONSTRUCTOR is a kind of MemberDeclaration */
  public static final int CONSTRUCTOR = 50;

  /** The STATICINIT is a kind of MemberDeclaration */
  public static final int STATICINIT = 32;

  /** The TYPE is a kind of MemberDeclaration */
  public static final int TYPE = 40;

  /** This is same as STATICINIT */
  public static final int STATICINITIALIZER = 32;

  /**
   * Returns which the kind of this class is.
   * This must be overridden.
   *
   * @return  FIELD if this is of FieldDeclaration class
   *          METHOD if this is of MethodDeclaration class
   *          CONSTRUCTOR if this is of ConstructorDeclaration class
   *          STATICINIT if this is of StaticInitializer class
   *          TYPE if this is of TypeDeclaration class
   */
  //public int getKind();

  /**
   * Tests if this object is of FieldDeclaration class.
   * This must be overridden.
   *
   * @return  true if this object is of FieldDeclaration class.
   */
  //public boolean isField();

  /**
   * Tests if this object is of MethodDeclaration class.
   * This must be overridden.
   *
   * @return  true if this object is of MethodDeclaration class.
   */
  //public boolean isMethod();

  /**
   * Tests if this object is of ConstructorDeclaration class.
   * This must be overridden.
   *
   * @return  true if this object is of ConstructorDeclaration class.
   */
  //public boolean isConstructor();

  /**
   * Tests if this object is of StaticInitializer class.
   * This must be overridden.
   *
   * @return  true if this object is of StaticInitializer class.
   */
  //public boolean isStaticInitializer();

  /**
   * Tests if this object is of TypeDeclaration class.
   * This must be overridden.
   *
   * @return  true if this object is of TypeDeclaration class.
   */
  //public boolean isType();

  /**
   *
   */
  public void writeCode();
  public boolean eq( ParseTree p );
  public boolean equals( ParseTree p );

}

