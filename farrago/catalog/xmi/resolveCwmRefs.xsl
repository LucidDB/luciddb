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

  <!-- Filter out the FemRef subpackage.  -->
  <xsl:template match="Model:Package[@name='FemRef']">
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
      name="refCwmClass" 
      select=
      "//Model:Package[@name='CwmRef']//Model:Class[@xmi.id=current()]" />
    <xsl:variable 
      name="refFemClass" 
      select=
      "//Model:Package[@name='FemRef']//Model:Class[@xmi.id=current()]" />
    <xsl:variable 
      name="refPrimitive" 
      select=
      "//Model:Package[@name='PrimitiveTypesRef']//Model:PrimitiveType[@xmi.id=current()]" />
    <xsl:choose>
      <xsl:when test="$refCwmClass">
        <xsl:variable 
          name="refCwmClassName" 
          select="$refCwmClass/@name"/>
        <xsl:variable 
          name="realCwmClass" 
          select=
          "//Model:Package[@name='CWM']//Model:Class[@name=$refCwmClassName]"/>
        <xsl:attribute name="xmi.idref">
          <xsl:value-of select="$realCwmClass/@xmi.id"/>
        </xsl:attribute>
      </xsl:when>
      <xsl:when test="$refFemClass">
        <xsl:variable 
          name="refFemClassName" 
          select="$refFemClass/@name"/>
        <xsl:variable 
          name="realFemClass" 
          select=
          "//Model:Package[@name='FEM']//Model:Class[@name=$refFemClassName]"/>
        <xsl:attribute name="xmi.idref">
          <xsl:value-of select="$realFemClass/@xmi.id"/>
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
