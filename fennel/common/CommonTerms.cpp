/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
