<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input FarragoExtMetamodel.xmi and -->
<!-- performs various transformations on it, producing -->
<!-- FarragoExtMetamodelTransformed.xsl.  -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="org.omg.xmi.namespace.Model"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- Prefix all xmi id's with q to avoid conflicts with CWM -->

  <xsl:template match="@xmi.id">
    <xsl:attribute name="xmi.id">
      <xsl:value-of select="concat('fem',.)"/>
    </xsl:attribute>
  </xsl:template>

  <xsl:template match="@xmi.idref">
    <xsl:attribute name="xmi.idref">
      <xsl:value-of select="concat('fem',.)"/>
    </xsl:attribute>
  </xsl:template>

  <!-- Add a Fem prefix to the name of each generated Java class -->
  <xsl:template
    match="Model:Package/Model:Namespace.contents/Model:Class">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <Model:Namespace.contents>
        <Model:Tag tagId='javax.jmi.substituteName'>
          <xsl:attribute name="elements">
            <xsl:value-of select="concat('fem',@xmi.id)"/>
          </xsl:attribute>
          <Model:Tag.values>
            <xsl:value-of select="concat('Fem',@name)"/>
          </Model:Tag.values>
        </Model:Tag>
      </Model:Namespace.contents>
    </xsl:copy>
  </xsl:template>

  <!-- Mark everything in the Fennel package as transient -->
  <xsl:template
    match="Model:Package[@name='FEM']/Model:Namespace.contents">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <xsl:for-each select=
        "Model:Package[@name='Fennel']/Model:Namespace.contents/Model:Class">
        <Model:Tag tagId='org.netbeans.mdr.transient' values='true'>
          <xsl:attribute name="elements">
            <xsl:value-of select="concat('fem',@xmi.id)"/>
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
