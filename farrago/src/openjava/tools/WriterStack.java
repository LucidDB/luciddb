/***
 *  WriterStack.java 1.0
 * this is a stack for Writer.
 * wrapper class of java.util.Stack
 *  
 * @see java.io.Writer
 * @see java.util.Stack
 * @version last updated: 06/11/97
 * @author  Michiaki Tatsubori
 ***/
/* package */
package openjava.tools;


/* import */
import java.lang.String;
import java.lang.System;
import java.util.Stack;
import java.io.OutputStream;
import java.io.Writer;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.util.Stack;
import java.util.EmptyStackException;


/**
 * Wrapper class of class Stack to specialize into Writer.
 **/
public class WriterStack
{
  private Stack stack = null;
  private static PrintWriter defaultWriter
      = new PrintWriter(
            new BufferedWriter(
              new OutputStreamWriter( System.out ) ) );

  /**
   * Constructs this stack with an element(standard output).
   **/
  public WriterStack() {
    stack = new Stack();
    stack.push( defaultWriter );
  }

  /**
   * Constructs this stack with the specified print writer.
   **/
  public WriterStack( PrintWriter writer ) {
    stack = new Stack();
    stack.push( writer );
  }

  /**
   * Constructs this stack with the specified writer.
   **/
  public WriterStack( Writer writer ) {
    stack = new Stack();
    stack.push( new PrintWriter( writer ) );
  }

  /**
   * Constructs this stack with the specified output stream.
   **/
  public WriterStack( OutputStream out ) {
    stack = new Stack();
    stack.push( new PrintWriter( new OutputStreamWriter( out ) ) );
  }

  /**
   * Looks at the print writer at the top of this stack 
   * without removing it from the stack. 
   *
   * @return the print writer at the top of this stack. 
   **/
  public PrintWriter peek() {
    try{
      return (PrintWriter)stack.peek();
    }catch( EmptyStackException ex ){
      System.err.println( ex );
      return defaultWriter;
    }
  }

  /**
   * Removes the print writer at the top of this stack and
   * returns that print writer as the value of this function. 
   * 
   * @return The object at the top of this stack. 
   **/
  public PrintWriter pop() {
    try{
      return (PrintWriter)stack.pop();
    }catch( EmptyStackException ex ){
      System.err.println( ex );
      return defaultWriter;
    }
  }

  /**
   * Pushes a print writer onto the top of this stack. 
   *
   * @param writer the print writer to be pushed onto this stack. 
   * @return the item argument. 
   **/
  public void push( PrintWriter writer ) {
    stack.push( writer );
  }

  /**
   * Pushes a print writer onto the top of this stack. 
   *
   * @param writer the writer to be pushed onto this stack. 
   * @return the item argument. 
   **/
  public void push( Writer writer ) {
    stack.push( new PrintWriter( writer ) );
  }

  /**
   * Pushes a print writer from the specified print stream
   * onto the top of this stack. 
   *
   * @param ostream the output stream to be pushed onto this stack. 
   * @return the item argument. 
   **/
  public void push( OutputStream ostream ) {
    stack.push( new PrintWriter( new OutputStreamWriter( ostream ) ) );
  }

  /**
   * Tests if this stack is empty. 
   *
   * @return true if this stack is empty; false otherwise. 
   **/
  public boolean empty() {
    return stack.empty();
  }

}
