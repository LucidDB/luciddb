/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.disruptivetech.farrago.sql.advise;

import java.io.*;

import java.util.*;
import java.util.regex.*;


/**
 * A simple parser that takes an incomplete and turn it into a syntactically
 * correct statement. It is used in the SQL editor user-interface.
 *
 * @author tleung
 * @version $Id$
 * @since Jan 31, 2004
 */
public class SqlSimpleParser
{
    //~ Static fields/initializers ---------------------------------------------

    private final static String subqueryRegex = "\\$subquery\\$";

    // patterns are made static, to amortize cost of compiling regexps
    static Pattern psq = Pattern.compile(subqueryRegex);
    static Pattern pparen =
        Pattern.compile(
            "\\([^()]*(SELECT)+[^()]*\\)",
            Pattern.CASE_INSENSITIVE);
    static Pattern pparensq =
        Pattern.compile(
            "\\([^()]*(SELECT)+[^()]*" + subqueryRegex + "$\\)",
            Pattern.CASE_INSENSITIVE);

    //~ Instance fields --------------------------------------------------------

    // Flags indicating precision/scale combinations
    private final String hintToken;
    final LinkedHashSet<String> keywords;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SqlSimpleParser
     */
    public SqlSimpleParser(String hintToken)
    {
        this.hintToken = hintToken;
        keywords = new LinkedHashSet<String>();
        keywords.add("select");
        keywords.add("from");
        keywords.add("join");
        keywords.add("on");
        keywords.add("where");
        keywords.add("group");
        keywords.add("having");
        keywords.add("order");
        keywords.add("");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Turns a partially completed or syntatically incorrect sql statement into
     * a simplified, valid one that can be passed into getCompletionHints()
     *
     * @param sql A partial or syntatically incorrect sql statement
     * @param cursor to indicate column position in the query at which
     * completion hints need to be retrieved.
     *
     * @return a completed, valid (and possibly simplified SQL statement
     */
    public String simplifySql(String sql, int cursor)
    {
        // introduce the hint token into the sql at the cursor pos
        if (cursor >= sql.length()) {
            sql += hintToken + " ";
        } else {
            String left = sql.substring(0, cursor);
            String right = sql.substring(cursor);
            sql = left + hintToken + " " + right;
        }
        return simplifySql(sql);
    }

    /**
     * Turns a partially completed or syntatically incorrect sql statement into
     * a simplified, valid one that can be validated
     *
     * @param sql A partial or syntatically incorrect sql statement
     *
     * @return a completed, valid (and possibly simplified) SQL statement
     */
    public String simplifySql(String sql)
    {
        // if there are subqueries, extract them and push them into a stack
        Stack<String> subqueries = new Stack<String>();
        stackSubqueries(sql, subqueries);

        // retrieve the top level query from the stack and go down the stack
        // to handle each subquery one by one
        if (subqueries.empty()) {
            return "";
        }
        String topLevelQuery = subqueries.pop();
        String result = handleSubQuery(topLevelQuery, subqueries);

        // remove the enclosing parentheses from the top level query
        return result.substring(1, result.length() - 1);
    }

    private String handleUnion(String sql)
    {
        String [] parts = sql.split("(?i)UNION( )+(ALL)?");
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].indexOf(hintToken) >= 0) {
                return parts[i];
            }
        }

        // no hint token found
        return sql;
    }

    private String handleSubQuery(String subquery, Stack<String> stack)
    {
        subquery = handleUnion(subquery);
        List<String> tokenList = tokenizeSubquery(subquery);
        Map<String, List<String>> buckets = bucketByKeyword(tokenList);

        //printBuckets(buckets);
        simplifyBuckets(buckets);

        //printBuckets(buckets);
        String simplesq = createNewSql(buckets);

        Matcher m = psq.matcher(simplesq);

        // this stack is used to reverse the ordering of same level subqueries
        Stack<String> reverseStack = new Stack<String>();
        while (m.find()) {
            String nextlevelQuery = stack.pop();
            nextlevelQuery = handleSubQuery(nextlevelQuery, stack);
            reverseStack.push(nextlevelQuery);
        }

        while (!reverseStack.empty()) {
            String sq = reverseStack.pop();
            simplesq = simplesq.replaceFirst(subqueryRegex, sq);
        }
        return "(" + simplesq.trim() + ")";
    }

    private String createNewSqlClause(List<String> members)
    {
        StringBuilder result = new StringBuilder();
        for (String member : members) {
            result.append(member).append(" ");
        }
        return result.toString();
    }

    private String createNewSql(Map<String, List<String>> buckets)
    {
        StringBuilder sql = new StringBuilder();
        for (String keyword : keywords) {
            List<String> entries = buckets.get(keyword);
            if (entries != null) {
                sql.append(keyword).append(" ").append(
                    createNewSqlClause(buckets.get(keyword)));
            }
        }
        return sql.toString();
    }

    private void stackSubqueries(String sql, Stack<String> stack)
    {
        // we're going depth first here, so that the innermost subquery
        // and its enclosing subqueries will be pushed into the stack
        // in that order

        Matcher msq = pparensq.matcher(sql);
        Matcher m = pparen.matcher(sql);

        boolean found = false;
        String remained = "";

        // giving preference to matching parentheses with the subquery token
        // inside to achieve depth-first
        if (msq.find()) {
            found = true;
            String matched = msq.group();

            // remove the enclosing parentheses
            matched = matched.substring(1, matched.length() - 1);
            stack.push(matched);
            if (matched.indexOf(hintToken) >= 0) {
                return;
            }
            remained = msq.replaceFirst(subqueryRegex);
        } else if (m.find()) {
            found = true;
            String matched = m.group();

            // remove the enclosing parentheses
            matched = matched.substring(1, matched.length() - 1);
            stack.push(matched);
            if (matched.indexOf(hintToken) >= 0) {
                return;
            }
            remained = m.replaceFirst(subqueryRegex);
        }
        if (!found) {
            stack.push(sql);
            return;
        }
        stackSubqueries(remained, stack);
    }

    // use StreamTokenizer to tokenize the subquery sql statement
    private List<String> tokenizeSubquery(String sql)
    {
        StreamTokenizer st = new StreamTokenizer(new StringReader(sql));

        boolean done = false;
        initializeSyntax(st);

        ArrayList<String> tokenList = new ArrayList<String>();
        while (!done) {
            int c = StreamTokenizer.TT_EOF;
            try {
                c = st.nextToken();
            } catch (IOException e) {
                break;
            }
            switch (c) {
            case StreamTokenizer.TT_EOF:
                tokenList.add("<EOF>");
                done = true;
                break;
            case StreamTokenizer.TT_EOL:
                tokenList.add("<EOL>");
                break;
            case StreamTokenizer.TT_WORD:
                tokenList.add(st.sval);
                break;
            case StreamTokenizer.TT_NUMBER:
                tokenList.add(Double.toString(st.nval));
                break;
            default:

                // Was just a regular character.
                break;
            }
        }
        return tokenList;
    }

    // enter the tokens list into different buckets keyed by its preceding
    // SQL keyword
    private Map<String, List<String>> bucketByKeyword(List<String> tokenList)
    {
        Map<String, List<String>> buckets = new HashMap<String, List<String>>();
        String curToken = "";
        List<String> curList = null;
        List<String> nokwList = new ArrayList<String>();
        for (String token : tokenList) {
            String tokenLc = token.toLowerCase();
            if (keywords.contains(tokenLc) || token.equals("<EOF>")) {
                if (!curToken.equals("")) {
                    buckets.put(curToken, curList);
                }
                curToken = tokenLc;
                curList = new ArrayList<String>();
            } else {
                if (curToken.equals("")) {
                    // this token does not follow any keyword
                    // this may not be a subquery, probably just an identifier
                    nokwList.add(token);
                } else {
                    curList.add(token);
                }
            }
        }
        buckets.put("", nokwList);
        return buckets;
    }

    private void printBuckets(HashMap<String, List<String>> buckets)
    {
        for (String keyword : buckets.keySet()) {
            List<String> entries = buckets.get(keyword);
            System.out.println("keyword = " + keyword);
            System.out.println(entries);
        }
    }

    // remove unnecessary (incomplete) keyword clause
    private void simplifyBuckets(Map<String, List<String>> buckets)
    {
        Set<String> toRemove = new HashSet<String>();

        for (String keyword : buckets.keySet()) {
            List<String> entries = buckets.get(keyword);
            SqlKw sqlkw = makeSqlKw(keyword, entries);
            List<String> valEntries = sqlkw.validate();
            if (valEntries == null) {
                toRemove.add(keyword);
            }
            buckets.put(
                keyword,
                sqlkw.validate());
        }

        // remove keywords with an empty clause
        for (String keyword : toRemove) {
            buckets.remove(keyword);
        }
    }

    // define the rules for the StreamTokenizer
    private void initializeSyntax(StreamTokenizer st)
    {
        st.resetSyntax();
        st.whitespaceChars(0, 32);
        st.wordChars(48, 122);
        st.ordinaryChars(33, 43);
        st.ordinaryChars(45, 45);
        st.ordinaryChars(58, 64);
        st.ordinaryChars(91, 96);
        st.ordinaryChars(123, 127);

        // we use $ for our specialized token => hence considered word character
        st.wordChars(36, 36);

        // " ' ( ) , . * = are considered word characters for a SQL statement
        st.wordChars(34, 34);
        st.wordChars(39, 42);
        st.wordChars(44, 44);
        st.wordChars(46, 46);
        st.wordChars(61, 61);
        st.wordChars(95, 95);
    }

    private SqlKw makeSqlKw(String keyword, List<String> entries)
    {
        if (keyword.equals("on")) {
            return new SqlKwOn(entries);
        } else if (keyword.equals("select")) {
            return new SqlKwSelect(entries);
        } else if (keyword.equals("from")) {
            return new SqlKwFrom(entries);
        } else if (keyword.equals("where")) {
            return new SqlKwWhere(entries);
        } else if (keyword.equals("group") || keyword.equals("order")) {
            return new SqlKwGroupOrder(entries);
        } else {
            return new SqlKw(entries);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    class SqlKw
    {
        protected List<String> entries;

        SqlKw(List<String> entries)
        {
            this.entries = entries;
        }

        List<String> validate()
        {
            if (entries.isEmpty()) {
                return null;
            } else {
                return entries;
            }
        }
    }

    class SqlKwOn
        extends SqlKw
    {
        private String dummyOp = "dummy";

        SqlKwOn(List<String> entries)
        {
            super(entries);
        }

        List<String> validate()
        {
            if (entries.isEmpty()) {
                return null;
            }
            List<String> validEntries = new ArrayList<String>();
            StringBuilder onClause = new StringBuilder();
            for (String entry : entries) {
                onClause.append(entry);
            }
            String [] operands = onClause.toString().split("=");

            if (operands.length >= 2) {
                validEntries.add(operands[0]);
                validEntries.add("=");
                validEntries.add(operands[1]);

                // if there're more operands the input SQL is invalid
                // anyway for having more than 1 '=' in 'on' clause
                // in that case we'll strip off the extra operands
                return validEntries;
            } else if (operands.length == 1) {
                validEntries.add(operands[0]);
                validEntries.add("=");
                validEntries.add(dummyOp);

                // if there's only 1 operand, put 'dummy' as the other one
                return validEntries;
            } else {
                // if there's no operands, there's no '='
                validEntries.add(onClause.toString());
                validEntries.add("=");
                validEntries.add(dummyOp);
                return validEntries;
            }
        }
    }

    class SqlKwList
        extends SqlKw
    {
        SqlKwList(List<String> entries)
        {
            super(entries);
        }

        List<String> validate()
        {
            List<String> validEntries = new ArrayList<String>();
            StringBuilder selectClause = new StringBuilder();
            for (int i = 0; i < entries.size(); i++) {
                String entry = entries.get(i);
                selectClause.append(entry);
                if (i < (entries.size() - 1)) {
                    selectClause.append(" ");
                }
            }
            String [] selectList = selectClause.toString().split(",");
            for (int i = 0; i < selectList.length; i++) {
                // remove leading and trailing space
                String entry = selectList[i].trim();

                // remove leading and trailing '.'
                entry = entry.replaceFirst("^\\.", "");
                entry = entry.replaceFirst("\\.$", "");
                validEntries.add(entry);
                if (i < (selectList.length - 1)) {
                    validEntries.add(",");
                }
            }
            return validEntries;
        }
    }

    class SqlKwSelect
        extends SqlKwList
    {
        SqlKwSelect(List<String> entries)
        {
            super(entries);
        }

        List<String> validate()
        {
            if (entries.isEmpty()) {
                entries.add("*");
                return entries;
            } else {
                return super.validate();
            }
        }
    }

    class SqlKwFrom
        extends SqlKwList
    {
        SqlKwFrom(List<String> entries)
        {
            super(entries);
        }

        List<String> validate()
        {
            if (entries.isEmpty()) {
                return null;
            } else {
                return super.validate();
            }
        }
    }

    class SqlKwGroupOrder
        extends SqlKwList
    {
        SqlKwGroupOrder(List<String> entries)
        {
            super(entries);
        }

        List<String> validate()
        {
            if (entries.isEmpty()) {
                return null;
            } else if (
                (entries.size() == 1)
                && entries.get(0).trim().equals("by"))
            {
                // a 'group' or 'order' keyword followed by 'by' but no
                // actual Sql Identifier
                return null;
            } else {
                for (String entry : entries) {
                    if (entry.indexOf(hintToken) >= 0) {
                        return super.validate();
                    }
                }
                return null;
            }
        }
    }

    class SqlKwWhere
        extends SqlKwList
    {
        SqlKwWhere(List<String> entries)
        {
            super(entries);
        }

        List<String> validate()
        {
            for (String entry : entries) {
                if (entry.indexOf(hintToken) >= 0) {
                    return super.validate();
                }
            }
            return null;
        }
    }
}

// End SqlSimpleParser.java
