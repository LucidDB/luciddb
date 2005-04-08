<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input an exported plugin model -->
<!-- and moves it under the Farrago package -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="org.omg.xmi.namespace.Model"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- Add container for top-level package -->
  <xsl:template match="XMI.content/Model:Package">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <Model:ModelElement.container>
        <Model:Package href='REPOS#Farrago'/>
      </Model:ModelElement.container>
    </xsl:copy>
  </xsl:template>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
