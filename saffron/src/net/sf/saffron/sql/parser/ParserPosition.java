/*
// $Id$
// Aspen dataflow server
// (C) Copyright 2004-2004 DisruptiveTech, Inc.
*/
package net.sf.saffron.sql.parser;

import net.sf.saffron.resource.SaffronResource;

/**
 * ParserPosition represents the position of a parsed
 * token within SQL statement text.
 *
 * @author Kinkoi Lo
 * @since Jun 1, 2004
 * @version $Id$
 **/
public class ParserPosition
{
    private int beginLine;

    private int beginColumn;

    /**
     * ParserPosition representing line one, character one. Use this if the
     * node doesn't correspond to a position in piece of SQL text. 
     */
    public static final ParserPosition ZERO = new ParserPosition(0,0);

    /**
    * Creates a new parser position.
    */
    public ParserPosition(int beginLine, int beginColumn)
    {
        this.beginLine = beginLine;
        this.beginColumn = beginColumn;
    }

    /**
     *
     * @return  line position of token beginning
     */
    public int getBeginLine()
    {
        return beginLine;
    }

    /**
     *
     * @param beginLine line position of token beginning
     */
    public void setBeginLine(int beginLine)
    {
        this.beginLine = beginLine;
    }

    /**
     *
     * @return column position of token beginning
     */
    public int getBeginColumn()
    {
        return beginColumn;
    }

    /**
     *
     * @param beginColumn column position of token beginning
     */
    public void setBeginColumn(int beginColumn)
    {
        this.beginColumn = beginColumn;
    }

    // implements Object
    public String toString()
    {
        return SaffronResource.instance().getParserContext(
                new Integer(beginLine),
                new Integer(beginColumn));
    }
}

// End ParserPosition.java