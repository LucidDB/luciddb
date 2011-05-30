/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package org.eigenbase.sql.pretty;

/**
 * Data structure to hold options for
 * {@link SqlPrettyWriter#setFormatOptions(SqlFormatOptions)}.
 * @author chard
 */
public class SqlFormatOptions
{
    private boolean alwaysUseParentheses = false;
    private boolean caseClausesOnNewLines = false;
    private boolean clauseStartsLine = true;
    private boolean keywordsLowercase = false;
    private boolean quoteAllIdentifiers = true;
    private boolean selectListItemsOnSeparateLines = false;
    private boolean whereListItemsOnSeparateLines = false;
    private boolean windowDeclarationStartsLine = true;
    private boolean windowListItemsOnSeparateLines = true;
    private int indentation = 4;
    private int lineLength = 0;

    /**
     * Constructs a set of default SQL format options.
     */
    public SqlFormatOptions()
    {
        super();
    }

    /**
     * COnstructs a complete set of SQL format options.
     * @param alwaysUseParentheses
     * @param caseClausesOnNewLines
     * @param clauseStartsLine
     * @param keywordsLowercase
     * @param quoteAllIdentifiers
     * @param selectListItemsOnSeparateLines
     * @param whereListItemsOnSeparateLines
     * @param windowDeclarationStartsLine
     * @param windowListItemsOnSeparateLines
     * @param indentation
     * @param lineLength
     */
    public SqlFormatOptions(
        boolean alwaysUseParentheses,
        boolean caseClausesOnNewLines,
        boolean clauseStartsLine,
        boolean keywordsLowercase,
        boolean quoteAllIdentifiers,
        boolean selectListItemsOnSeparateLines,
        boolean whereListItemsOnSeparateLines,
        boolean windowDeclarationStartsLine,
        boolean windowListItemsOnSeparateLines,
        int indentation,
        int lineLength)
    {
        this();
        this.alwaysUseParentheses = alwaysUseParentheses;
        this.caseClausesOnNewLines = caseClausesOnNewLines;
        this.clauseStartsLine = clauseStartsLine;
        this.keywordsLowercase = keywordsLowercase;
        this.quoteAllIdentifiers = quoteAllIdentifiers;
        this.selectListItemsOnSeparateLines = selectListItemsOnSeparateLines;
        this.whereListItemsOnSeparateLines = whereListItemsOnSeparateLines;
        this.windowDeclarationStartsLine = windowDeclarationStartsLine;
        this.windowListItemsOnSeparateLines = windowListItemsOnSeparateLines;
        this.indentation = indentation;
        this.lineLength = lineLength;
    }

    public boolean isAlwaysUseParentheses()
    {
        return alwaysUseParentheses;
    }

    public void setAlwaysUseParentheses(boolean alwaysUseParentheses)
    {
        this.alwaysUseParentheses = alwaysUseParentheses;
    }

    public boolean isCaseClausesOnNewLines()
    {
        return caseClausesOnNewLines;
    }

    public void setCaseClausesOnNewLines(boolean caseClausesOnNewLines)
    {
        this.caseClausesOnNewLines = caseClausesOnNewLines;
    }

    public boolean isClauseStartsLine()
    {
        return clauseStartsLine;
    }

    public void setClauseStartsLine(boolean clauseStartsLine)
    {
        this.clauseStartsLine = clauseStartsLine;
    }

    public boolean isKeywordsLowercase()
    {
        return keywordsLowercase;
    }

    public void setKeywordsLowercase(boolean keywordsLowercase)
    {
        this.keywordsLowercase = keywordsLowercase;
    }

    public boolean isQuoteAllIdentifiers()
    {
        return quoteAllIdentifiers;
    }

    public void setQuoteAllIdentifiers(boolean quoteAllIdentifiers)
    {
        this.quoteAllIdentifiers = quoteAllIdentifiers;
    }

    public boolean isSelectListItemsOnSeparateLines()
    {
        return selectListItemsOnSeparateLines;
    }

    public void setSelectListItemsOnSeparateLines(
        boolean selectListItemsOnSeparateLines)
    {
        this.selectListItemsOnSeparateLines = selectListItemsOnSeparateLines;
    }

    public boolean isWhereListItemsOnSeparateLines()
    {
        return whereListItemsOnSeparateLines;
    }

    public void setWhereListItemsOnSeparateLines(
        boolean whereListItemsOnSeparateLines)
    {
        this.whereListItemsOnSeparateLines = whereListItemsOnSeparateLines;
    }

    public boolean isWindowDeclarationStartsLine()
    {
        return windowDeclarationStartsLine;
    }

    public void setWindowDeclarationStartsLine(
        boolean windowDeclarationStartsLine)
    {
        this.windowDeclarationStartsLine = windowDeclarationStartsLine;
    }

    public boolean isWindowListItemsOnSeparateLines()
    {
        return windowListItemsOnSeparateLines;
    }

    public void setWindowListItemsOnSeparateLines(
        boolean windowListItemsOnSeparateLines)
    {
        this.windowListItemsOnSeparateLines = windowListItemsOnSeparateLines;
    }

    public int getLineLength()
    {
        return lineLength;
    }

    public void setLineLength(int lineLength)
    {
        this.lineLength = lineLength;
    }

    public int getIndentation()
    {
        return indentation;
    }

    public void setIndentation(int indentation)
    {
        this.indentation = indentation;
    }
}

// End SqlFormatOptions.java
