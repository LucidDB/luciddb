<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input an XMI document containing the -->
<!-- combination of CWM and FEM and resolves cross-model references. -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="omg.org/mof.Model/1.3"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- Filter out the CwmRef subpackage.  -->
  <xsl:template match="Model:Package[@name='CwmRef']">
  </xsl:template>

  <!-- Filter out the FenelRef subpackage.  -->
  <xsl:template match="Model:Package[@name='FennelRef']">
  </xsl:template>

  <xsl:template match="Model:Package[@name='PrimitiveTypesRef']">
  </xsl:template>

  <xsl:template match="Model:Import[@name='PrimitiveTypesRef']">
  </xsl:template>

  <xsl:template match="Model:Package[@name='FEME']">
      <xsl:apply-templates select="Model:Namespace.contents/Model:Package" />
  </xsl:template>

  <!-- When we see an idref which refers to a class in package
       CwmRef, remap it to the id of the real CWM class instead.  -->
  <xsl:template match="@xmi.idref">
    <xsl:variable 
      name="refClass" 
      select=
      "//Model:Package[@name='CwmRef']//Model:Class[@xmi.id=current()]" />
    <xsl:variable 
      name="refFenelClass" 
      select=
      "//Model:Package[@name='FennelRef']//Model:Class[@xmi.id=current()]" />
    <xsl:variable 
      name="refPrimitive" 
      select=
      "//Model:Package[@name='PrimitiveTypesRef']//Model:PrimitiveType[@xmi.id=current()]" />
    <xsl:choose>
      <xsl:when test="$refClass">
        <xsl:variable 
          name="refClassName" 
          select="$refClass/@name"/>
        <xsl:variable 
          name="realClass" 
          select=
          "//Model:Package[@name='CWM']//Model:Class[@name=$refClassName]"/>
        <xsl:attribute name="xmi.idref">
          <xsl:value-of select="$realClass/@xmi.id"/>
        </xsl:attribute>
      </xsl:when>
      <xsl:when test="$refFenelClass">
        <xsl:variable 
          name="refFenelClassName" 
          select="$refFenelClass/@name"/>
        <xsl:variable 
          name="realFenelClass" 
          select=
          "//Model:Package[@name='Fennel']//Model:Class[@name=$refFenelClassName]"/>
        <xsl:attribute name="xmi.idref">
          <xsl:value-of select="$realFenelClass/@xmi.id"/>
        </xsl:attribute>
      </xsl:when>
      <xsl:when test="$refPrimitive">
        <xsl:variable 
          name="refPrimitiveName" 
          select="$refPrimitive/@name"/>
        <xsl:variable 
          name="realPrimitiveType" 
          select=
          "//Model:Package[@name='PrimitiveTypes']//Model:PrimitiveType[@name=$refPrimitiveName]"/>
        <xsl:attribute name="xmi.idref">
          <xsl:value-of select="$realPrimitiveType/@xmi.id"/>
        </xsl:attribute>
      </xsl:when>
      <xsl:otherwise>
        <xsl:copy/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>


  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
