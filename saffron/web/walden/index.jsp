<%@page contentType="text/html"%>
<%--
// $Id$
// 
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// jhyde, 25 May, 2002
--%>
<%
  String[] queries = net.sf.saffron.walden.Main.getQueries();
%>

<html>
<head>
<style>
.{
font-family:"verdana";
}

</style>
<title>Walden</title>
</head>
<body>
    <form action="waldenservlet">
    <table>
        <tr>
            <td>
                <select onchange="document.all.item('commandArea').innerHTML=this.value">
                <% for (int i=0; i<queries.length; i++) { %>
                    <option value="<%= queries[i] %>">Query #<%= i %>: <%= queries[i] %></option>
                <% } %>
                </select>
            </td>
        </tr>
        <tr>
            <td>
                <textarea id='commandArea' name="commandString" rows=10 cols=80><%
if (request.getAttribute("commandString") != null) {
    out.print(request.getAttribute("commandString"));
} %></textarea>
            </td>
        </tr>
        <tr>
            <td>
                <input type="submit" value="Run Java">
            </td>
        </tr>
<% if (request.getAttribute("result") != null) { %>
        <tr>
            <td>
                Result: <pre><%= request.getAttribute("result") %></pre>
            </td>
        </tr>
<% } %>
    </table>
    </form>
</body>
</html>
