/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.web.applet;

import net.sf.saffron.util.SaffronProperties;
import net.sf.saffron.walden.Interpreter;
import net.sf.saffron.walden.PrintHandler;

import java.applet.Applet;

import java.awt.*;
import java.awt.event.*;

import java.io.*;


/**
 * A <code>WaldenApplet</code> is ...
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Jun 6, 2002
 */
public class WaldenApplet extends Applet
{
    //~ Static fields/initializers --------------------------------------------

    private static final int pad = 8;
    private static final String newline = System.getProperty("line.separator");

    //~ Instance fields -------------------------------------------------------

    private Interpreter interpreter;
    private TextArea outputArea;
    private TextArea textArea;

    //~ Methods ---------------------------------------------------------------

    public void init()
    {
        super.init();
        SaffronProperties properties = SaffronProperties.instance();
        properties.classDir.set("d:/saffron/classes");
        properties.javaDir.set("d:/saffron/src/examples");
        properties.javaCompilerClass.set("openjava.ojc.DynamicJavaCompiler");
        final PipedWriter inWriter = startInterpreter();
        setBackground(Color.white);
        setLayout(null);
        textArea = new TextArea();
        textArea.addKeyListener(
            new KeyListener() {
                public void keyTyped(KeyEvent e)
                {
                    char c = e.getKeyChar();
                    if (c == '\n') {
                        try {
                            String text = textArea.getText();
                            if (text.startsWith("\n")) {
                                text = text.substring(1);
                            }
                            textArea.setText("");
                            writeToOutput(">> " + text + newline);
                            inWriter.write(text);
                        } catch (IOException e1) {
                        }
                    }
                }

                public void keyPressed(KeyEvent e)
                {
                }

                public void keyReleased(KeyEvent e)
                {
                }
            });
        add(textArea);
        outputArea = new TextArea();
        add(outputArea);
        addComponentListener(
            new ComponentAdapter() {
                public void componentResized(ComponentEvent e)
                {
                    resizeContents();
                }
            });
        resizeContents();
    }

    void writeToOutput(String s)
    {
        outputArea.append(s);
    }

    private void resizeContents()
    {
        int width = getWidth();
        int height = getHeight();
        textArea.setBounds(pad,pad,width - (2 * pad),(height - (pad * 3)) / 2);
        int top = textArea.getY() + textArea.getHeight() + pad;
        outputArea.setBounds(pad,top,width - (2 * pad),height - pad - top);
    }

    /**
     * Returns the stream to write commands into.
     */
    private PipedWriter startInterpreter()
    {
        interpreter = new Interpreter();
        final PipedWriter inWriter = new PipedWriter();
        final PipedReader inReader;
        try {
            inReader = new PipedReader(inWriter);
        } catch (IOException e) {
            throw new RuntimeException(e + " while creating input stream");
        }
        Writer outWriter =
            new Writer() {
                public void write(char [] cbuf,int off,int len)
                    throws IOException
                {
                    writeToOutput(new String(cbuf,off,len));
                }

                public void flush() throws IOException
                {
                }

                public void close() throws IOException
                {
                }
            };
        final PrintWriter pw = new PrintWriter(outWriter);
        new Thread() {
                public void run()
                {
                    try {
                        interpreter.run(
                            inReader,
                            new PrintHandler(interpreter,pw,true));
                    } catch (Throwable e) {
                        pw.println("Interpreter received exception: ");
                        e.printStackTrace(pw);
                        pw.println();
                    }
                }
            }.start();
        return inWriter;
    }
}


// End WaldenApplet.java
