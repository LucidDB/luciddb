<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet clean up the PrimitiveTypes package in the XMI template-->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="org.omg.xmi.namespace.Model"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- Delete import package-->
  <xsl:template match="Model:Import[@name='PrimitiveTypes']" />

  <!-- Rename the PrimitiveTypes package to PrimitiveTypeRef -->
  <xsl:template match="Model:Package[@name='PrimitiveTypes']/@name" >
    <xsl:attribute name="name">PrimitiveTypesRef</xsl:attribute>
  </xsl:template>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
