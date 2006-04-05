<?xml version="1.0"?> 
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="html" indent="no"/>
<xsl:template match="test-log">
<html>
<head>
<title>Blackhawk HTML Report</title>
<style type="text/css">
.unittests-sectionheader { background-color:#000066; font-family:arial,helvetica,sans-serif; font-size:10pt; color:#FFFFFF; }
.unittests-oddrow { background-color:#CCCCCC }
.unittests-data { font-family:arial,helvetica,sans-serif; font-size:8pt; color:#000000;background-color:#CCCCCC }
.unittests-error { font-family:arial,helvetica,sans-serif; font-size:8pt; color:#901090; }
.unittests-failure { font-family:arial,helvetica,sans-serif; font-size:8pt; color:#FF0000; }
.unittests-title { font-family:arial,helvetica,sans-serif; font-size:9pt; font-weight: bold; color:#000080; background-color:#CCDDDD; }
.unittests-error-title { font-family:arial,helvetica,sans-serif; font-size:9pt; font-weight: bold; color:#901090; background-color:#CCDDDD; }
.unittests-failure-title { font-family:arial,helvetica,sans-serif; font-size:9pt; color:#FF0000; font-weight: bold; background-color:#CCDDDD; }
</style>
</head>
<body>
<center>
<table>
<tr class="unittests-title">
<th>testsuitename</th>
<th>testcasename</th>
<th>basename</th>
<th>result</th>
<th>duration</th>
<th>exectime</th>
<th>testpath</th>
<th>isdone</th>
<th>execution-output</th>
</tr>
<xsl:apply-templates select="test-result"/>
</table>
</center>
</body>
</html>
</xsl:template>
<xsl:template match="test-result">
<tr class="unittests-data">
<td><xsl:value-of select="test-case/@testsuitemodifiers"/><xsl:value-of select="test-case/@testsuitename"/></td>
<td><xsl:value-of select="test-case/@testcasename"/></td>
<td><xsl:value-of select="test-case/@basename"/></td>
<td><xsl:value-of select="@result"/></td>
<td><xsl:value-of select="@duration"/></td>
<td><xsl:value-of select="@exectime"/></td>
<td><xsl:value-of select="test-case/@testpath"/></td>
<td><xsl:value-of select="@isdone"/></td>
<td><xsl:value-of select="test-case/execution-output/output-details/."/></td>
<!--
exectime:          <xsl:value-of select="@exectime"/>
result:            <xsl:value-of select="@result"/>
isdone:            <xsl:value-of select="@isdone"/>
duration:          <xsl:value-of select="@duration"/>ms
<xsl:apply-templates select="test-case"/></xsl:template>
<xsl:template match="test-case">testcasename:      <xsl:value-of select="@testcasename"/>
basename:          <xsl:value-of select="@basename"/>
testpath:          <xsl:value-of select="@testpath"/>
execution-output:  <xsl:value-of select="execution-output/output-details/."/>
-->
</tr>
</xsl:template>

</xsl:stylesheet>
