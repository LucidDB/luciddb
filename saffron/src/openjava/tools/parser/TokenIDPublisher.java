package openjava.tools.parser;


public interface TokenIDPublisher {

  public static final int EOF =			ParserConstants.EOF;

  public static final int ABSTRACT =		ParserConstants.ABSTRACT;
  public static final int BOOLEAN =		ParserConstants.BOOLEAN;
  public static final int BREAK =		ParserConstants.BREAK;
  public static final int BYTE =		ParserConstants.BYTE;
  public static final int CASE =		ParserConstants.CASE;
  public static final int CATCH =		ParserConstants.CATCH;
  public static final int CHAR =		ParserConstants.CHAR;
  public static final int CLASS =		ParserConstants.CLASS;
  public static final int CONTINUE =		ParserConstants.CONTINUE;
  public static final int DEFAULT =		ParserConstants._DEFAULT;
  public static final int DO =			ParserConstants.DO;
  public static final int DOUBLE =		ParserConstants.DOUBLE;
  public static final int ELSE =		ParserConstants.ELSE;
  public static final int EXTENDS =		ParserConstants.EXTENDS;
  public static final int FALSE =		ParserConstants.FALSE;
  public static final int FINAL =		ParserConstants.FINAL;
  public static final int FINALLY =		ParserConstants.FINALLY;
  public static final int FLOAT =		ParserConstants.FLOAT;
  public static final int FOR =			ParserConstants.FOR;
  public static final int IF =			ParserConstants.IF;
  public static final int IMPLEMENTS =		ParserConstants.IMPLEMENTS;
  public static final int IMPORT =		ParserConstants.IMPORT;
  public static final int INSTANCEOF =		ParserConstants.INSTANCEOF;
  public static final int INT =			ParserConstants.INT;
  public static final int INTERFACE =		ParserConstants.INTERFACE;
  public static final int LONG =		ParserConstants.LONG;
  public static final int NATIVE =		ParserConstants.NATIVE;
  public static final int NEW =			ParserConstants.NEW;
  public static final int NULL =		ParserConstants.NULL;
  public static final int PACKAGE =		ParserConstants.PACKAGE;
  public static final int PRIVATE =		ParserConstants.PRIVATE;
  public static final int PROTECTED =		ParserConstants.PROTECTED;
  public static final int PUBLIC =		ParserConstants.PUBLIC;
  public static final int RETURN =		ParserConstants.RETURN;
  public static final int SHORT =		ParserConstants.SHORT;
  public static final int STATIC =		ParserConstants.STATIC;
  public static final int SUPER =		ParserConstants.SUPER;
  public static final int SWITCH =		ParserConstants.SWITCH;
  public static final int SYNCHRONIZED =	ParserConstants.SYNCHRONIZED;
  public static final int THIS =		ParserConstants.THIS;
  public static final int THROW =		ParserConstants.THROW;
  public static final int THROWS =		ParserConstants.THROWS;
  public static final int TRANSIENT =		ParserConstants.TRANSIENT;
  public static final int TRUE =		ParserConstants.TRUE;
  public static final int TRY =			ParserConstants.TRY;
  public static final int VOID =		ParserConstants.VOID;
  public static final int VOLATILE =		ParserConstants.VOLATILE;
  public static final int WHILE =		ParserConstants.WHILE;
  public static final int METACLASS =		ParserConstants.METACLASS;
  public static final int INSTANTIATES =	ParserConstants.INSTANTIATES;

  public static final int INTEGER_LITERAL
      =	  ParserConstants.INTEGER_LITERAL;
  public static final int LONG_LITERAL
      =   ParserConstants.LONG_LITERAL;
  public static final int DOUBLE_LITERAL
      =   ParserConstants.DOUBLE_FLOATING_POINT_LITERAL;
  public static final int FLOAT_LITERAL
      =	  ParserConstants.FLOATING_POINT_LITERAL;
  public static final int CHARACTER_LITERAL
      =	  ParserConstants.CHARACTER_LITERAL;
  public static final int STRING_LITERAL
      =   ParserConstants.STRING_LITERAL;

  public static final int IDENTIFIER =		ParserConstants.IDENTIFIER;
  public static final int LPAREN =		ParserConstants.LPAREN;
  public static final int RPAREN =		ParserConstants.RPAREN;
  public static final int LBRACE =		ParserConstants.LBRACE;
  public static final int RBRACE =		ParserConstants.RBRACE;
  public static final int LBRACKET =		ParserConstants.LBRACKET;
  public static final int RBRACKET =		ParserConstants.RBRACKET;
  public static final int SEMICOLON =		ParserConstants.SEMICOLON;
  public static final int COMMA =		ParserConstants.COMMA;
  public static final int DOT =			ParserConstants.DOT;
  public static final int ASSIGN =		ParserConstants.ASSIGN;
  public static final int GREATER =		ParserConstants.GT;
  public static final int LESS =		ParserConstants.LT;
  public static final int BANG =		ParserConstants.BANG;
  public static final int TILDE =		ParserConstants.TILDE;
  public static final int HOOK =		ParserConstants.HOOK;
  public static final int COLON =		ParserConstants.COLON;
  public static final int EQUAL =		ParserConstants.EQ;
  public static final int LESS_EQUAL =		ParserConstants.LE;
  public static final int GREATER_EQUAL =	ParserConstants.GE;
  public static final int NOT_EQUAL =		ParserConstants.NE;
  public static final int CONDITIONAL_OR =	ParserConstants.SC_OR;
  public static final int CONDITIONAL_AND =	ParserConstants.SC_AND;
  public static final int INCREMENT =		ParserConstants.INCR;
  public static final int DECREMENT =		ParserConstants.DECR;
  public static final int PLUS =		ParserConstants.PLUS;
  public static final int MINUS =		ParserConstants.MINUS;
  public static final int STAR =		ParserConstants.STAR;
  public static final int SLASH =		ParserConstants.SLASH;
  public static final int BIT_AND =		ParserConstants.BIT_AND;
  public static final int BIT_OR =		ParserConstants.BIT_OR;
  public static final int XOR =			ParserConstants.XOR;
  public static final int REM =			ParserConstants.REM;
  public static final int LSHIFT =		ParserConstants.LSHIFT;
  public static final int RSIGNEDSHIFT =	ParserConstants.RSIGNEDSHIFT;
  public static final int RUNSIGNEDSHIFT =	ParserConstants.RUNSIGNEDSHIFT;
  public static final int PLUSASSIGN =	ParserConstants.PLUSASSIGN;
  public static final int MINUSASSIGN =	ParserConstants.MINUSASSIGN;
  public static final int STARASSIGN =	ParserConstants.STARASSIGN;
  public static final int SLASHASSIGN =	ParserConstants.SLASHASSIGN;
  public static final int ANDASSIGN =	ParserConstants.ANDASSIGN;
  public static final int ORASSIGN =	ParserConstants.ORASSIGN;
  public static final int XORASSIGN =	ParserConstants.XORASSIGN;
  public static final int REMASSIGN =	ParserConstants.REMASSIGN;
  public static final int LSHIFTASSIGN =	ParserConstants.LSHIFTASSIGN;
  public static final int RSIGNEDSHIFTASSIGN
      =   ParserConstants.RSIGNEDSHIFTASSIGN;
  public static final int RUNSIGNEDSHIFTASSIGN
      =   ParserConstants.RUNSIGNEDSHIFTASSIGN;

}
