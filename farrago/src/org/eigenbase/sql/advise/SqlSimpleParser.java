/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import java.io.StreamTokenizer; 
import java.io.StringBufferInputStream; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack; 
import java.util.LinkedHashSet; 
import java.util.HashSet; 
import java.util.HashMap; 
import java.util.Iterator; 
import java.util.regex.*;

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
    final String hintToken;
    final String subqueryRegex;
    final LinkedHashSet keywords;

    //~ Constructors ----------------------------------------------------------
    /**
     * Creates a SqlSimpleParser
     */
    public SqlSimpleParser()
    {
        hintToken = "$suggest$ ";
        subqueryRegex = "\\$subquery\\$";
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
            sql += hintToken;
        } else {
            String left = sql.substring(0, cursor);
            String right = sql.substring(cursor);
            sql = left + hintToken + right;
        }

        // if there are subqueries, extract them and push them into a stack
        Stack subqueries = new Stack();
        stackSubqueries(sql, subqueries);

        String result = new String();

        // retrieve the top level query from the stack and go down the stack
        // to handle each subquery one by one
        if (!subqueries.empty()) {
            String topLevelQuery = (String) subqueries.pop();
            result = handleSubQuery(topLevelQuery, subqueries);
        }

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

        Pattern p = Pattern.compile(subqueryRegex);
        Matcher m = p.matcher(simplesq);

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
        String result = new String();
        while (i.hasNext()) {
            String word = (String) i.next();
            result += word;
            result += " ";
        }
        return result;
    }

    private String createNewSql(HashMap buckets)
    {
        Iterator i = keywords.iterator(); 
        String sql = new String();
        while (i.hasNext()) {
            String keyword = (String) i.next();
            List entries = (List) buckets.get(keyword);
            if (entries != null) {
                sql += keyword + " " + 
                    createNewSqlClause((List)buckets.get(keyword));
            }
        }
        return sql;
    }

    private void stackSubqueries(String sql, Stack stack)
    {
        // we're going depth first here, so that the innermost subquery
        // and its enclosing subqueries will be pushed into the stack 
        // in that order
        Pattern psq = Pattern.compile("\\([^()]*"+subqueryRegex+"$\\)");
        Matcher msq = psq.matcher(sql);
            
        Pattern p = Pattern.compile("\\([^()]*\\)");
        Matcher m = p.matcher(sql);

        boolean found = false;
        String remained = new String();

        // giving preference to matching parentheses with the subquery token
        // inside to achieve depth-first
        if (msq.find()) {
            found = true;
            String matched = msq.group();
            stack.push(matched);
            remained = msq.replaceFirst(subqueryRegex);
        }
        else if (m.find()) {
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
        StringBufferInputStream sis = new StringBufferInputStream(sql);
        StreamTokenizer st = new StreamTokenizer(sis);

        boolean done = false;
        initializeSyntax(st);

        ArrayList tokenList = new ArrayList();
        while (!done) {
            int c = StreamTokenizer.TT_EOF;
            try {
                c = st.nextToken();
            }
            catch (IOException e) {
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
                    boolean wasQuote = false;
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
                }
                else if (keyword.equals("select")) {
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
}


// End SqlSimpleParser.java
