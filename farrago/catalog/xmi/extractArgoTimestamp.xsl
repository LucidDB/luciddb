<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input an ArgoUML .uml XML document -->
<!-- and extracts just the model timestamp. -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:UML="org.omg.xmi.namespace.UML"
  >
  <xsl:output method="text" indent="no" />

  <xsl:template match="XMI">
    <xsl:text>fem.timestamp=</xsl:text><xsl:value-of select="@timestamp"/>
    <xsl:text>
</xsl:text>
  </xsl:template>

  <xsl:template match="/ | @* | node()">
    <xsl:apply-templates select="@* | node()" />
  </xsl:template>

</xsl:stylesheet>
