/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

FENNEL_BEGIN_CPPFILE("$Id$");

/**
 * The official SQL-92 standard (ISO/IEC 9075:1992).  To reference
 * this standard from comments on methods that implement its rules, use
 * the format
 * <code>&lt;SectionId&gt; [ ItemType &lt;ItemId&gt; ]</code>,
 * where
 *
 *<ul>
 *
 *<li><code>SectionId</code> is the numbered or named section in the table of
 *contents, e.g. "Section 4.18.9" or "Annex A"
 *
 *
 *<li><code>ItemType</code> is one of { Table, Syntax Rule, Access Rule,
 *General Rule, or Leveling Rule }
 *
 *<li><code>ItemId</code> is a dotted path expression to the specific item
 *
 *</ul>
 *
 * For example,
 *
 *<pre><code>SQL92 Section 11.4 Syntax Rule 7.c</code></pre>
 *
 * is a well-formed reference to the rule for the default character
 * set to use for column definitions of character type.
 */
struct SQL92
{
    // NOTE:  dummy class for doxygen
};

/**
 * The official SQL:1999 standard (ISO/IEC 9075:1999), which
 * is broken up into five parts.  To reference
 * this standard from comments on methods that implement its rules, use
 * the format
 * <code>&lt;PartId&gt; &lt;SectionId&gt;
 * [ ItemType &lt;ItemId&gt; ]</code>, where
 *
 *<ul>
 *
 *<li><code>PartId</code> is the numbered part (up to Part 5)
 *
 *<li><code>SectionId</code> is the numbered or named section in the part's
 *table of contents, e.g. "Section 4.18.9" or "Annex A"
 *
 *<li><code>ItemType</code> is one of { Table, Syntax Rule, Access Rule,
 *General Rule, or Conformance Rule }
 *
 *<li><code>ItemId</code> is a dotted path expression to the specific item
 *
 *</ul>
 *
 * For example,
 *
 *<pre><code>SQL99 Part 2 Section 11.4 Syntax Rule 7.b</code></pre>
 *
 * is a well-formed reference to the rule for the default character
 * set to use for column definitions of character type.
 */
struct SQL99
{
    // NOTE:  dummy class for doxygen
};

/**
 * The official SQL:2003 standard (ISO/IEC 9075:2003), which
 * is broken up into numerous parts.  To reference
 * this standard from comments on methods that implement its rules, use
 * the format
 * <code>&lt;PartId&gt; &lt;SectionId&gt;
 * [ ItemType &lt;ItemId&gt; ]</code>, where
 *
 *<ul>
 *
 *<li><code>PartId</code> is the numbered part
 *
 *<li><code>SectionId</code> is the numbered or named section in the part's
 *table of contents, e.g. "Section 4.11.2" or "Annex A"
 *
 *<li><code>ItemType</code> is one of { Table, Syntax Rule, Access Rule,
 *General Rule, or Conformance Rule }
 *
 *<li><code>ItemId</code> is a dotted path expression to the specific item
 *
 *</ul>
 *
 * For example,
 *
 *<pre><code>SQL2003 Part 2 Section 11.4 Syntax Rule 10.b</code></pre>
 *
 * is a well-formed reference to the rule for the default character
 * set to use for column definitions of character type.
 */
struct SQL2003
{
    // NOTE:  dummy class for doxygen
};

FENNEL_END_CPPFILE("$Id$");

// End CommonTerms.cpp
