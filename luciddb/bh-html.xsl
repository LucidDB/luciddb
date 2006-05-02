<?xml version="1.0"?> 
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="html" indent="no"/>
<xsl:template match="test-log">
<html>
<head>
<title>Blackhawk HTML Report</title>
<style type="text/css">
.table_header { background-color:#000066; font-family:arial,helvetica,sans-serif; font-size:11pt; color:#FFFFFF; font-weight:bold}
.table_info { background-color:#AAAACC; font-family:arial,helvetica,sans-serif; font-size:10pt; color:#000000; }
.unittests-sectionheader { background-color:#000066; font-family:arial,helvetica,sans-serif; font-size:10pt; color:#FFFFFF; font-style:bold}
.unittests-oddrow { background-color:#CCCCCC }
.unittests-data { font-family:arial,helvetica,sans-serif; font-size:8pt; color:#000000;background-color:#CCCCCC }
.unittests-error { font-family:arial,helvetica,sans-serif; font-size:8pt; color:#901090; }
.unittests-failure { font-family:arial,helvetica,sans-serif; font-size:8pt; color:#FF0000; }
.unittests-title { font-family:arial,helvetica,sans-serif; font-size:9pt; font-weight: bold; color:#000080; background-color:#CCDDDD; }
.unittests-error-title { font-family:arial,helvetica,sans-serif; font-size:9pt; font-weight: bold; color:#901090; background-color:#CCDDDD; }
.unittests-failure-title { font-family:arial,helvetica,sans-serif; font-size:9pt; color:#FF0000; font-weight: bold; background-color:#CCDDDD; }
.h2 { font-family:arial,helvetica,sans-serif; font-size:13pt; color:#000000 }
.info { font-family:arial,helvetica,sans-serif; font-size:10pt; font-style:italic;  color:#000000; }
</style>
</head>
<body>
<h2>Blackhawk Testrun Report</h2>
<table width="20%">
<tr class="table_header">
<td>runid:</td><td class="table_info"><xsl:value-of select="@runid"/></td>
</tr>
<tr class="table_header">
<td>execaccount: </td><td class="table_info"><xsl:value-of select="header-info/@execaccount"/></td>
</tr>
<tr class="table_header">
<td>execdate:</td><td class="table_info"> <xsl:value-of select="header-info/@execdate"/></td>
</tr>
<tr class="table_header">
<td>hostname:</td><td class="table_info"> <xsl:value-of select="@hostname"/></td>
</tr>
</table>
<P>
</P>

<!--
<div class="info">runid: <xsl:value-of select="@runid"/></div><//>
<div class="info">execaccount: <xsl:value-of select="header-info/@execaccount"/></div><br/>
<div class="info">execdate: <xsl:value-of select="header-info/@execdate"/></div><br/>
<div class="info">hostname: <xsl:value-of select="@hostname"/></div><br/>
-->
<center>
<table width="80%">
<tr class="unittests-title">
<th>testsuitename</th>
<th>testcasename</th>
<th>basename</th>
<th>result</th>
<th>duration</th>
<th>exectime</th>
<th>testpath</th>
<th>isdone</th>
<th>execution-output<br></br>(500 char max)</th>
</tr>
<xsl:apply-templates select="test-result"/>
</table>
</center>
</body>
</html>
</xsl:template>
<xsl:template match="test-result">
<tr class="unittests-data">
  <xsl:if test="position() mod 2 != 1">
    <xsl:attribute  name="style">background-color:#dddddd</xsl:attribute>
  </xsl:if>
<td><xsl:value-of select="test-case/@testsuitename"/>(<xsl:value-of select="test-case/@testsuitemodifiers"/>)</td>
<td><xsl:value-of select="test-case/@testcasename"/></td>
<td><xsl:value-of select="test-case/@basename"/></td>
<td><xsl:value-of select="@result"/></td>
<td><xsl:value-of select="@duration"/></td>
<td><xsl:value-of select="@exectime"/></td>
<td><xsl:value-of select="test-case/@testpath"/></td>
<td><xsl:value-of select="@isdone"/></td>
<td><xsl:value-of select="substring(execution-output/output-details/.,0,500)"/></td>
</tr>
</xsl:template>

</xsl:stylesheet>
