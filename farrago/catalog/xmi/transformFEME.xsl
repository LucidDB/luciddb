<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input ExtMetamodel.xmi and -->
<!-- performs various transformations on it, producing -->
<!-- ExtMetamodelTransformed.xsl.  -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="org.omg.xmi.namespace.Model"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- Rename the PrimitiveTypes package to PrimitiveTypeRef -->
  <xsl:template match="Model:Package[@name='PrimitiveTypes']/@name" >
    <xsl:attribute name="name">PrimitiveTypesRef</xsl:attribute>
  </xsl:template>

  <!-- Rename import package-->
  <xsl:template match="Model:Import[@name='PrimitiveTypes']/@name" >
    <xsl:attribute name="name">PrimitiveTypesRef</xsl:attribute>
  </xsl:template>


  <!-- Prefix all xmi id's with q to avoid conflicts with CWM -->

  <xsl:template match="@xmi.id">
    <xsl:attribute name="xmi.id">
      <xsl:value-of select="concat('feme',.)"/>
    </xsl:attribute>
  </xsl:template>

  <xsl:template match="@xmi.idref">
    <xsl:attribute name="xmi.idref">
      <xsl:value-of select="concat('feme',.)"/>
    </xsl:attribute>
  </xsl:template>

  <!-- Add a Feme prefix to the name of each generated Java class -->
  <xsl:template
    match="Model:Package/Model:Namespace.contents/Model:Class">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <Model:Namespace.contents>
        <Model:Tag tagId='javax.jmi.substituteName'>
          <xsl:attribute name="elements">
            <xsl:value-of select="concat('feme',@xmi.id)"/>
          </xsl:attribute>
          <Model:Tag.values>
            <xsl:value-of select="concat('Feme',@name)"/>
          </Model:Tag.values>
        </Model:Tag>
      </Model:Namespace.contents>
    </xsl:copy>
  </xsl:template>

  <!-- Mark everything in the Fennel package as transient -->
  <xsl:template
    match="Model:Package[@name='FEME']/Model:Namespace.contents">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <xsl:for-each select=
        "Model:Package[@name='Fennel']/Model:Namespace.contents/Model:Class">
        <Model:Tag tagId='org.netbeans.mdr.transient' values='true'>
          <xsl:attribute name="elements">
            <xsl:value-of select="concat('feme',@xmi.id)"/>
          </xsl:attribute>
        </Model:Tag>
      </xsl:for-each>
    </xsl:copy>
  </xsl:template>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
