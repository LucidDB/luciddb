/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

package org.eigenbase.sql.advise;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A simple parser that takes an incomplete and turn it into a syntactically
 * correct statement.  It is used in the SQL editor user-interface.
 *
 * @author tleung
 * @since Jan 31, 2004
 * @version $Id$
 **/
public class SqlSimpleParser
{
    //~ Static fields/initializers --------------------------------------------

    // Flags indicating precision/scale combinations

    //~ Instance fields -------------------------------------------------------
    private final String hintToken;
    private final static String subqueryRegex = "\\$subquery\\$";
    
    // patterns are made static, to amortize cost of compiling regexps
    static Pattern psq = Pattern.compile(subqueryRegex);
    static Pattern pparen = Pattern.compile("\\([^()]*\\)");
    static Pattern pparensq = Pattern.compile("\\([^()]*"+subqueryRegex+"$\\)");
    final LinkedHashSet keywords;

    //~ Constructors ----------------------------------------------------------
    /**
     * Creates a SqlSimpleParser
     */
    public SqlSimpleParser(String hintToken)
    {
        this.hintToken = hintToken;
        keywords = new LinkedHashSet();
        keywords.add("select");
        keywords.add("from");
        keywords.add("join");
        keywords.add("on");
        keywords.add("where");
        keywords.add("order by");
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Turn a partially completed or syntatically incorrect sql statement into
     * a simplified, valid one that can be passed into getCompletionHints()
     *
     * @param sql A partial or syntatically incorrect sql statement
     * @param cursor to indicate column position in the query at which
     * completion hints need to be retrieved.
     *
     * @return a completed, valid (and possibly simplified SQL statement
     *
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

        // if there are subqueries, extract them and push them into a stack
        Stack subqueries = new Stack();
        stackSubqueries(sql, subqueries);


        // retrieve the top level query from the stack and go down the stack
        // to handle each subquery one by one
        if (subqueries.empty()) {
            return "";
        }
        String topLevelQuery = (String) subqueries.pop();
        String result = handleSubQuery(topLevelQuery, subqueries);

        // remove the enclosing parentheses from the top level query
        return result.substring(1, result.length()-1);
    }

    private String handleSubQuery(String subquery, Stack stack)
    {
        List tokenList = tokenizeSubquery(subquery);
        HashMap buckets = bucketByKeyword(tokenList);
        //printBuckets(buckets);
        simplifyBuckets(buckets);
        //printBuckets(buckets);
        String simplesq = createNewSql(buckets);

        Matcher m = psq.matcher(simplesq);

        // this stack is used to reverse the ordering of same level subqueries
        Stack reverseStack = new Stack();
        while (m.find()) {
            String nextlevelQuery = (String) stack.pop();
            nextlevelQuery = handleSubQuery(nextlevelQuery, stack);
            reverseStack.push(nextlevelQuery);
        }

        while (!reverseStack.empty()) {
            String sq = (String) reverseStack.pop();
            simplesq = simplesq.replaceFirst(subqueryRegex, sq);
        }
        return "(" + simplesq.trim() + ")";
    }

    private String createNewSqlClause(List members)
    {
        Iterator i = members.iterator();
        StringBuffer result = new StringBuffer();
        while (i.hasNext()) {
            result.append(i.next()).append(" ");
        }
        return result.toString();
    }

    private String createNewSql(HashMap buckets)
    {
        Iterator i = keywords.iterator();
        StringBuffer sql = new StringBuffer();
        while (i.hasNext()) {
            String keyword = (String) i.next();
            List entries = (List) buckets.get(keyword);
            if (entries != null) {
                sql.append(keyword).append(" ").append(
                    createNewSqlClause((List)buckets.get(keyword)));
            }
        }
        return sql.toString();
    }

    private void stackSubqueries(String sql, Stack stack)
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
            stack.push(matched);
            remained = msq.replaceFirst(subqueryRegex);
        } else if (m.find()) {
            found = true;
            String matched = m.group();
            stack.push(matched);
            remained = m.replaceFirst(subqueryRegex);
        }
        if (!found) {
            stack.push(sql);
            return;
        }
        stackSubqueries(remained, stack);
    }

    // use StreamTokenizer to tokenize the subquery sql statement
    private List tokenizeSubquery(String sql)
    {
        StreamTokenizer st = new StreamTokenizer(new StringReader(sql));

        boolean done = false;
        initializeSyntax(st);

        ArrayList tokenList = new ArrayList();
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
    private HashMap bucketByKeyword(List tokenList)
    {
        HashMap buckets = new HashMap();
        Iterator i = tokenList.iterator();
        String curToken = "";
        ArrayList curList = null;
        while (i.hasNext()) {
            String token = (String) i.next();
            if (keywords.contains(token) || token.equals("<EOF>")) {
                if (curToken != "") {
                    buckets.put(curToken, curList);
                }
                curToken = token;
                curList = new ArrayList();
            } else {
                curList.add(token);
            }
        }
        return buckets;
    }

    private void printBuckets(HashMap buckets)
    {
        Iterator keywords = buckets.keySet().iterator();
        while (keywords.hasNext()) {
            String keyword  = (String) keywords.next();
            ArrayList entries = (ArrayList) buckets.get(keyword);
            System.out.println("keyword = " + keyword);
            System.out.println(entries);
        }
    }

    // remove unnecessary (incomplete) keyword clause
    private void simplifyBuckets(HashMap buckets)
    {
        Iterator keywords = buckets.keySet().iterator();
        HashSet toRemove = new HashSet();
        while (keywords.hasNext()) {
            String keyword  = (String) keywords.next();
            ArrayList entries = (ArrayList) buckets.get(keyword);
            if (entries.isEmpty()) {
                if (keyword.equals("from")) {
                    // giving up right now if from list is empty
                    return;
                } else if (keyword.equals("select")) {
                    entries.add("*");
                } else {
                    toRemove.add(keyword);
                }
            }
        }
        Iterator i = toRemove.iterator();
        while (i.hasNext()) {
            String keyword = (String) i.next();
            buckets.remove(keyword);
        }
        validate(buckets);
    }

    private void validate(HashMap buckets)
    {
        Iterator keywords = buckets.keySet().iterator();
        while (keywords.hasNext()) {
            String keyword  = (String) keywords.next();
            ArrayList entries = (ArrayList) buckets.get(keyword);
            if (keyword.equals("on")) {
                SqlKwOn sqlon = new SqlKwOn(entries);
                buckets.put(keyword, sqlon.validate());
            }
        }
    }

    // define the rules for the StreamTokenizer
    private void initializeSyntax(StreamTokenizer st)
    {
        st.resetSyntax();
        st.whitespaceChars(0,32);
        st.wordChars(48, 122);
        st.ordinaryChars(33, 43);
        st.ordinaryChars(45, 45);
        st.ordinaryChars(58, 64);
        st.ordinaryChars(91, 96);
        st.ordinaryChars(123, 127);
        // we use $ for our specialized token => hence considered word character
        st.wordChars(36, 36);
        // , . * = are considered word characters for a SQL statement
        st.wordChars(42, 42);
        st.wordChars(44, 44);
        st.wordChars(46, 46);
        st.wordChars(61, 61);
    }

    abstract class SqlKw {
        protected List entries;

        SqlKw(List entries) {
            this.entries = entries;
        }

        List validate() {
            return entries;
        }
    }
    class SqlKwOn extends SqlKw {
        
        private String dummyOp = "dummy";

        SqlKwOn(List entries) {
            super(entries);
        }

        List validate() {
            ArrayList validEntries = new ArrayList();
            StringBuffer onClause = new StringBuffer();
            for (int i = 0; i < entries.size(); i++) {
                String entry = (String) entries.get(i);
                onClause.append(entry);
            }
            //System.out.println("onClause:" + onClause.toString());
            String [] operands = onClause.toString().split("=");
            /* 
            System.out.println("splitted:");
            for (int i = 0; i < operands.length; i++) {
                System.out.println(operands[i]);
            }*/
                
            if (operands.length >= 2) {
                //System.out.println("2 operands or more"); 
                validEntries.add(operands[0]);
                validEntries.add("=");
                validEntries.add(operands[1]);
                // if there're more operands the input SQL is invalid 
                // anyway for having more than 1 '=' in 'on' clause
                // in that case we'll strip off the extra operands
                return validEntries;
            } else if (operands.length == 1) {
              //  System.out.println("1 operands");
                validEntries.add(operands[0]);
                validEntries.add("=");
                validEntries.add(dummyOp);
                // if there's only 1 operand, put 'dummy' as the other one
                return validEntries;
            } else {
               // System.out.println("0 operands");
                // if there's no operands, there's no '='
                validEntries.add(onClause.toString());
                validEntries.add("=");
                validEntries.add(dummyOp);
                return validEntries;
            }
        }
    }
                
}


// End SqlSimpleParser.java
