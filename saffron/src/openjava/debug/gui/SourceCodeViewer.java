package openjava.debug.gui;


import java.awt.*;
import java.awt.event.*;
import java.util.Hashtable;

import javax.swing.*;
import javax.swing.text.*;
import javax.swing.event.*;
import javax.swing.undo.*;


public class SourceCodeViewer extends JFrame {
    public static String newline = "\n";
    JTextPane textPane;
    public DefaultStyledDocument[] lsd = new DefaultStyledDocument[3];
    JTextArea changeLog;
    Hashtable actions;
    private Object lock;

    public SourceCodeViewer( Object lock ) {
	this();
	this.lock = lock;
	addWindowListener( new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                dispose();
            }
            public void windowActivated(WindowEvent e) {
                textPane.requestFocus();
            }
        });
	pack();
	setVisible( true );
    }

    SourceCodeViewer() {
        /*some initial setup*/
        super( "OpenJava SourceCodeViewer" );

	JTabbedPane tabPane = new JTabbedPane();

	final String[] tabtitle
	    = new String[] { "init  ", "callee", "caller" };
        /*Create the document for the text area.*/
	for (int i = 0; i < 3; ++i) {
	    /*Create the text pane and configure it.*/
	    lsd[i] = new DefaultStyledDocument();
	    JTextPane tp = new JTextPane( lsd[i] );
	    if (i == 0)  textPane = tp;
	    tp.setMargin(new Insets(5,5,5,5));
	    JScrollPane scrollPane = new JScrollPane( tp );
	    scrollPane.setPreferredSize( new Dimension( 400, 600 ) );
	    tabPane.add( tabtitle[i], scrollPane );
	}


        /*Create the text area for the status log and configure it.*/
        changeLog = new JTextArea(4, 80);
        changeLog.setFont( new Font( "Courier", Font.PLAIN, 10 ) );
        changeLog.setEditable(false);
        JScrollPane scrollPaneForLog = new JScrollPane(changeLog);

        /*Create a split pane for the change log and the text area.*/
        JSplitPane splitPane = new JSplitPane(
                                       JSplitPane.VERTICAL_SPLIT,
                                       tabPane, scrollPaneForLog );
        splitPane.setOneTouchExpandable(true);

        /*Create the status area.*/
        JPanel statusPane = new JPanel(new GridLayout(1, 1));
        CaretListenerLabel caretListenerLabel =
                new CaretListenerLabel("Caret Status");
        statusPane.add(caretListenerLabel);

        /*Add the components to the frame.*/
        JPanel contentPane = new JPanel(new BorderLayout());
        contentPane.add(splitPane, BorderLayout.CENTER);
        contentPane.add(statusPane, BorderLayout.SOUTH);
        setContentPane(contentPane);

        /*Set up the menu bar.*/
        createActionTable(textPane);
        JMenu fileMenu = createFileMenu();
        JMenu editMenu = createEditMenu();
        JMenu styleMenu = createStyleMenu();
        JMenuBar mb = new JMenuBar();
        mb.add(fileMenu);
        mb.add(editMenu);
        mb.add(styleMenu);
        setJMenuBar(mb);
    }

    /*This listens for and reports caret movements.*/
    protected class CaretListenerLabel extends JLabel
        implements CaretListener
    {
        public CaretListenerLabel (String label) {
            super(label);
        }

        public void caretUpdate(CaretEvent e) {
            /*Get the location in the text.*/
            int dot = e.getDot();
            int mark = e.getMark();
            if (dot == mark) {  /*no selection */
                try {
                    Rectangle caretCoords = textPane.modelToView(dot);
                    /*Convert it to view coordinates.*/
                    setText("caret: text position: " + dot
                            + ", view location = ["
                            + caretCoords.x + ", "
                            + caretCoords.y + "]"
                            + newline);
                } catch (BadLocationException ble) {
                    setText("caret: text position: " + dot + newline);
                }
            } else if (dot < mark) {
                setText("selection from: " + dot
                        + " to " + mark + newline);
            } else {
                setText("selection from: " + mark
                        + " to " + dot + newline);
            }
        }
    }

    /*Create the file menu.*/
    protected JMenu createFileMenu() {
        JMenu menu = new JMenu("File");

	JMenuItem item_step = new JMenuItem( "Step" );
	item_step.addActionListener( new ActionListener() {
	    public void actionPerformed( ActionEvent event ) {
		if (lock == null)  return;
                try {
                    synchronized ( lock ) { lock.notifyAll(); }
                } catch ( IllegalMonitorStateException e ) {
                    e.printStackTrace();
                }
	    }
	} );
        menu.add( item_step );

        menu.addSeparator();

	JMenuItem item_quit = new JMenuItem( "Quit" );
	item_quit.addActionListener( new ActionListener() {
	    public void actionPerformed( ActionEvent event ) {
		System.exit( 0 );
	    }
	} );
        menu.add( item_quit );

        return menu;
    }

    /*Create the edit menu.*/
    protected JMenu createEditMenu() {
        JMenu menu = new JMenu("Edit");

        /*These actions come from the default editor kit.*/
        /*Get the ones we want and stick them in the menu.*/
        menu.add(getActionByName(DefaultEditorKit.cutAction));
        menu.add(getActionByName(DefaultEditorKit.copyAction));
        menu.add(getActionByName(DefaultEditorKit.pasteAction));

        menu.addSeparator();

        menu.add(getActionByName(DefaultEditorKit.selectAllAction));
        return menu;
    }

    /*Create the style menu.*/
    protected JMenu createStyleMenu() {
        JMenu menu = new JMenu("Style");

        Action action = new StyledEditorKit.BoldAction();
        action.putValue(Action.NAME, "Bold");
        menu.add(action);

        action = new StyledEditorKit.ItalicAction();
        action.putValue(Action.NAME, "Italic");
        menu.add(action);

        action = new StyledEditorKit.UnderlineAction();
        action.putValue(Action.NAME, "Underline");
        menu.add(action);

        menu.addSeparator();

        menu.add(new StyledEditorKit.FontSizeAction("12", 12));
        menu.add(new StyledEditorKit.FontSizeAction("14", 14));
        menu.add(new StyledEditorKit.FontSizeAction("18", 18));

        menu.addSeparator();

        menu.add(new StyledEditorKit.FontFamilyAction("Serif",
                                                      "Serif"));
        menu.add(new StyledEditorKit.FontFamilyAction("SansSerif",
                                                      "SansSerif"));

        menu.addSeparator();

        menu.add(new StyledEditorKit.ForegroundAction("Red",
                                                      Color.red));
        menu.add(new StyledEditorKit.ForegroundAction("Green",
                                                      Color.green));
        menu.add(new StyledEditorKit.ForegroundAction("Blue",
                                                      Color.blue));
        menu.add(new StyledEditorKit.ForegroundAction("Black",
                                                      Color.black));

        return menu;
    }

    /*The following two methods allow us to find an*/
    /*action provided by the editor kit by its name.*/
    private void createActionTable(JTextComponent textComponent) {
        actions = new Hashtable();
        Action[] actionsArray = textComponent.getActions();
        for (int i = 0; i < actionsArray.length; i++) {
            Action a = actionsArray[i];
            actions.put(a.getValue(Action.NAME), a);
        }
    }

    private Action getActionByName(String name) {
        return (Action) actions.get(name);
    }

    public static SimpleAttributeSet regularAttr = new SimpleAttributeSet();
    static {
        StyleConstants.setFontFamily( regularAttr, "Courier" );
        StyleConstants.setFontSize( regularAttr, 12 );
    }

    public DefaultStyledDocument getEntry( int num ) {
	return lsd[num];
    }

    public void setSourceCode( String text ) {
	setSourceCode( 0, text );
    }

    public void setSourceCode( int num, String text ) {
        try {
	    System.err.println( "setting" );
            lsd[num].remove( 0, lsd[num].getLength() );
	    System.err.println( "removed" );
            lsd[num].insertString( 0, text, regularAttr );
	    System.err.println( "inserted" );
        } catch ( BadLocationException e ) {
            e.printStackTrace();
        }
    }

    /*The standard main method.*/
    public static void main(String[] args) {
        final SourceCodeViewer frame = new SourceCodeViewer();
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                System.exit(0);
            }
            public void windowActivated(WindowEvent e) {
                frame.textPane.requestFocus();
            }
        });

        /*Put the initial text into the text pane.*/
        /*frame.initDocument();*/
        initDocument( frame.lsd[0] );

        frame.pack();
        frame.setVisible(true);
    }

    protected static void initDocument( DefaultStyledDocument lsd ) {
        String initString[] = {
            "public class VectorStack implements Stack {",
            "    public void push() {}",
            "    public Object pop() {}",
            "    public int size() {}",
            "    public boolean isEmpty()",
            "}" };

        SimpleAttributeSet[] attrs = initAttributes(initString.length);

        try {
            for (int i = 0; i < initString.length; i ++) {
                lsd.insertString(lsd.getLength(), initString[i] + newline,
                        attrs[i]);
            }
        } catch (BadLocationException ble) {
            System.err.println("Couldn't insert initial text.");
        }
    }

    protected static SimpleAttributeSet[] initAttributes(int length) {
        /*Hard-code some attributes.*/
        SimpleAttributeSet[] attrs = new SimpleAttributeSet[length];

        attrs[0] = new SimpleAttributeSet();
        StyleConstants.setFontFamily(attrs[0], "Courier");
        StyleConstants.setFontSize(attrs[0], 12);

        attrs[1] = new SimpleAttributeSet(attrs[0]);
        StyleConstants.setBold(attrs[1], true);

        attrs[2] = new SimpleAttributeSet(attrs[0]);
        StyleConstants.setItalic(attrs[2], true);

        attrs[3] = new SimpleAttributeSet(attrs[0]);
        StyleConstants.setFontSize(attrs[3], 12);

        attrs[4] = new SimpleAttributeSet(attrs[0]);
        StyleConstants.setFontSize(attrs[4], 12);

        attrs[5] = new SimpleAttributeSet(attrs[0]);
        StyleConstants.setForeground(attrs[5], Color.red);

        return attrs;
    }
}
