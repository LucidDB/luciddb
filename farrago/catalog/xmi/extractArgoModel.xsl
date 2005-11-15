<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input an ArgoUML .uml XML document -->
<!-- and extracts just the XMI portion. -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:UML="org.omg.xmi.namespace.UML"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- Filter out uml and arg stuff:  skip to XMI child -->
  <xsl:template match="uml">
    <xsl:apply-templates select="XMI" />
  </xsl:template>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
