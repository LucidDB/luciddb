/*
 * RestorableTokenSource.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.syntax;


import openjava.mop.Environment;
import openjava.tools.parser.Token;


/**
 * A {@link TokenSource} which can be back-tracked.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class RestorableTokenSource implements TokenSource
{
    private TokenSource tokenSource;
    private Environment env;
    private int pointer = 0;
    private int offset = 0;

    public RestorableTokenSource( TokenSource src, Environment env ) {
        this.tokenSource = src;
        this.env = env;
    }

    public RestorableTokenSource( TokenSource src ) {
        this( src, null );
    }

    public Environment getEnvironment() {
        return ((env == null) ? tokenSource.getEnvironment() : env);
    }

    public void assume() { offset = pointer; }
    public void restore() { pointer = offset; }
    public void fix() {
        for (int i = offset; i < pointer; ++i) {
             tokenSource.getNextToken();
        }
        pointer = 0;  offset = 0;
    }
    public Token getNextToken() {
        return tokenSource.getToken( ++pointer );
    }

    public Token getToken( int i ) {
        return tokenSource.getToken( pointer + i );
    }

}
