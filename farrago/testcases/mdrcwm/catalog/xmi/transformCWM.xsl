<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input an XMI document containing the CWM -->
<!-- model definition and applies some fixes to it. -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="omg.org/mof.Model/1.3"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- Make everything public -->
  <xsl:template match="@visibility">
    <xsl:attribute name="visibility">public_vis</xsl:attribute>
  </xsl:template>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
