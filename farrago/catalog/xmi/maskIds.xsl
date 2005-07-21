<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input an XMI file and -->
<!-- regenerates it with all ids and idrefs masked -->
<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  >
  <xsl:output method="xml" indent="yes" />

  <xsl:template match="@xmi.id">
    <xsl:attribute name="xmi.id">XXX</xsl:attribute>
  </xsl:template>

  <xsl:template match="@xmi.idref">
    <xsl:attribute name="xmi.idref">XXX</xsl:attribute>
  </xsl:template>

  <xsl:template match="@timestamp">
    <xsl:attribute name="timestamp">XXX</xsl:attribute>
  </xsl:template>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
