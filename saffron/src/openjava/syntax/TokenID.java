/*
 * TokenID.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.syntax;


import openjava.tools.parser.TokenIDPublisher;


/**
 * Contains constants for lexical tokens.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public interface TokenID
{

  public static final int EOF =         TokenIDPublisher.EOF;

  public static final int ABSTRACT =        TokenIDPublisher.ABSTRACT;
  public static final int BOOLEAN =     TokenIDPublisher.BOOLEAN;
  public static final int BREAK =       TokenIDPublisher.BREAK;
  public static final int BYTE =        TokenIDPublisher.BYTE;
  public static final int CASE =        TokenIDPublisher.CASE;
  public static final int CATCH =       TokenIDPublisher.CATCH;
  public static final int CHAR =        TokenIDPublisher.CHAR;
  public static final int CLASS =       TokenIDPublisher.CLASS;
  public static final int CONTINUE =        TokenIDPublisher.CONTINUE;
  public static final int DEFAULT =     TokenIDPublisher.DEFAULT;
  public static final int DO =          TokenIDPublisher.DO;
  public static final int DOUBLE =      TokenIDPublisher.DOUBLE;
  public static final int ELSE =        TokenIDPublisher.ELSE;
  public static final int EXTENDS =     TokenIDPublisher.EXTENDS;
  public static final int FALSE =       TokenIDPublisher.FALSE;
  public static final int FINAL =       TokenIDPublisher.FINAL;
  public static final int FINALLY =     TokenIDPublisher.FINALLY;
  public static final int FLOAT =       TokenIDPublisher.FLOAT;
  public static final int FOR =         TokenIDPublisher.FOR;
  public static final int IF =          TokenIDPublisher.IF;
  public static final int IMPLEMENTS =      TokenIDPublisher.IMPLEMENTS;
  public static final int IMPORT =      TokenIDPublisher.IMPORT;
  public static final int INSTANCEOF =      TokenIDPublisher.INSTANCEOF;
  public static final int INT =         TokenIDPublisher.INT;
  public static final int INTERFACE =       TokenIDPublisher.INTERFACE;
  public static final int LONG =        TokenIDPublisher.LONG;
  public static final int NATIVE =      TokenIDPublisher.NATIVE;
  public static final int NEW =         TokenIDPublisher.NEW;
  public static final int NULL =        TokenIDPublisher.NULL;
  public static final int PACKAGE =     TokenIDPublisher.PACKAGE;
  public static final int PRIVATE =     TokenIDPublisher.PRIVATE;
  public static final int PROTECTED =       TokenIDPublisher.PROTECTED;
  public static final int PUBLIC =      TokenIDPublisher.PUBLIC;
  public static final int RETURN =      TokenIDPublisher.RETURN;
  public static final int SHORT =       TokenIDPublisher.SHORT;
  public static final int STATIC =      TokenIDPublisher.STATIC;
  public static final int SUPER =       TokenIDPublisher.SUPER;
  public static final int SWITCH =      TokenIDPublisher.SWITCH;
  public static final int SYNCHRONIZED =    TokenIDPublisher.SYNCHRONIZED;
  public static final int THIS =        TokenIDPublisher.THIS;
  public static final int THROW =       TokenIDPublisher.THROW;
  public static final int THROWS =      TokenIDPublisher.THROWS;
  public static final int TRANSIENT =       TokenIDPublisher.TRANSIENT;
  public static final int TRUE =        TokenIDPublisher.TRUE;
  public static final int TRY =         TokenIDPublisher.TRY;
  public static final int VOID =        TokenIDPublisher.VOID;
  public static final int VOLATILE =        TokenIDPublisher.VOLATILE;
  public static final int WHILE =       TokenIDPublisher.WHILE;
  public static final int METACLASS =       TokenIDPublisher.METACLASS;
  public static final int INSTANTIATES =    TokenIDPublisher.INSTANTIATES;

  public static final int INTEGER_LITERAL
      =  TokenIDPublisher.INTEGER_LITERAL;
  public static final int LONG_LITERAL
      =  TokenIDPublisher.LONG_LITERAL;
  public static final int DOUBLE_LITERAL
      =  TokenIDPublisher.DOUBLE_LITERAL;
  public static final int FLOAT_LITERAL
      =  TokenIDPublisher.FLOAT_LITERAL;
  public static final int CHARACTER_LITERAL
      =  TokenIDPublisher.CHARACTER_LITERAL;
  public static final int STRING_LITERAL
      =  TokenIDPublisher.STRING_LITERAL;

  public static final int IDENTIFIER =      TokenIDPublisher.IDENTIFIER;

  public static final int LPAREN =      TokenIDPublisher.LPAREN;
  public static final int RPAREN =      TokenIDPublisher.RPAREN;
  public static final int LBRACE =      TokenIDPublisher.LBRACE;
  public static final int RBRACE =      TokenIDPublisher.RBRACE;
  public static final int LBRACKET =        TokenIDPublisher.LBRACKET;
  public static final int RBRACKET =        TokenIDPublisher.RBRACKET;
  public static final int SEMICOLON =       TokenIDPublisher.SEMICOLON;
  public static final int COMMA =       TokenIDPublisher.COMMA;
  public static final int DOT =         TokenIDPublisher.DOT;
  public static final int ASSIGN =      TokenIDPublisher.ASSIGN;
  public static final int GREATER =     TokenIDPublisher.GREATER;
  public static final int LESS =        TokenIDPublisher.LESS;
  public static final int BANG =        TokenIDPublisher.BANG;
  public static final int TILDE =       TokenIDPublisher.TILDE;
  public static final int HOOK =        TokenIDPublisher.HOOK;
  public static final int COLON =       TokenIDPublisher.COLON;
  public static final int EQUAL =       TokenIDPublisher.EQUAL;
  public static final int LESS_EQUAL =      TokenIDPublisher.LESS_EQUAL;
  public static final int GREATER_EQUAL =   TokenIDPublisher.GREATER_EQUAL;
  public static final int NOT_EQUAL =       TokenIDPublisher.NOT_EQUAL;
  public static final int CONDITIONAL_OR
      =  TokenIDPublisher.CONDITIONAL_OR;
  public static final int CONDITIONAL_AND
      =  TokenIDPublisher.CONDITIONAL_AND;
  public static final int INCREMENT =       TokenIDPublisher.INCREMENT;
  public static final int DECREMENT =       TokenIDPublisher.DECREMENT;
  public static final int PLUS =        TokenIDPublisher.PLUS;
  public static final int MINUS =       TokenIDPublisher.MINUS;
  public static final int STAR =        TokenIDPublisher.STAR;
  public static final int SLASH =       TokenIDPublisher.SLASH;
  public static final int BIT_AND =     TokenIDPublisher.BIT_AND;
  public static final int BIT_OR =      TokenIDPublisher.BIT_OR;
  public static final int XOR =         TokenIDPublisher.XOR;
  public static final int REM =         TokenIDPublisher.REM;
  public static final int LSHIFT =      TokenIDPublisher.LSHIFT;
  public static final int RSIGNEDSHIFT
      =  TokenIDPublisher.RSIGNEDSHIFT;
  public static final int RUNSIGNEDSHIFT
      =  TokenIDPublisher.RUNSIGNEDSHIFT;
  public static final int PLUSASSIGN =  TokenIDPublisher.PLUSASSIGN;
  public static final int MINUSASSIGN = TokenIDPublisher.MINUSASSIGN;
  public static final int STARASSIGN =  TokenIDPublisher.STARASSIGN;
  public static final int SLASHASSIGN = TokenIDPublisher.SLASHASSIGN;
  public static final int ANDASSIGN =   TokenIDPublisher.ANDASSIGN;
  public static final int ORASSIGN =    TokenIDPublisher.ORASSIGN;
  public static final int XORASSIGN =   TokenIDPublisher.XORASSIGN;
  public static final int REMASSIGN =   TokenIDPublisher.REMASSIGN;
  public static final int LSHIFTASSIGN =TokenIDPublisher.LSHIFTASSIGN;
  public static final int RSIGNEDSHIFTASSIGN
      =  TokenIDPublisher.RSIGNEDSHIFTASSIGN;
  public static final int RUNSIGNEDSHIFTASSIGN
      =  TokenIDPublisher.RUNSIGNEDSHIFTASSIGN;

}
