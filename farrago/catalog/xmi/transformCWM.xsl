<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input an XMI document containing the CWM -->
<!-- model definition and transforms it into the form desired for -->
<!-- usage as the Farrago system catalog.  -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="omg.org/mof.Model/1.3"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- location of FEM XMI passed in from ant -->
  <xsl:param name="femXmiFilename"/>

  <!-- location of Farrago extension model XMI from ant -->
  <xsl:param name="extXmiFilename"/>

  <!-- Introduce some top-level packages, and insert FEM as a peer to CWM. -->
  <xsl:template match="XMI/XMI.content">
    <xsl:copy>
      <Model:Package xmi.id='z1' name='Farrago'
        annotation='Top-level package' isRoot='false' isLeaf='false' 
        isAbstract='false' visibility='public_vis'>
        <Model:Namespace.contents>
          <Model:Tag tagId='javax.jmi.packagePrefix' elements='z1'>
            <Model:Tag.values>net.sf</Model:Tag.values>
          </Model:Tag>
          <Model:Tag tagId='org.netbeans.implPackagePrefix' elements='z1'>
            <Model:Tag.values>net.sf</Model:Tag.values>
          </Model:Tag>
          <Model:Package name='CWM'
            annotation='CWM Container' isRoot='false' isLeaf='false' 
            isAbstract='false' visibility='public_vis'>
            <Model:Namespace.contents>
              <xsl:apply-templates select="@* | node()" />
            </Model:Namespace.contents>
          </Model:Package>
          <xsl:copy-of select="document($femXmiFilename)/XMI/XMI.content/*"/>
          <xsl:copy-of select="document($extXmiFilename)/XMI/XMI.content/*"/>
        </Model:Namespace.contents>
      </Model:Package>
    </xsl:copy>
  </xsl:template>

  <!-- Filter for just the relevant CWM subpackages.  -->
  <xsl:template match="XMI.content/Model:Package">
    <xsl:if test="contains('Behavioral,Core,Instance,DataTypes,KeysIndexes,Relational',@name)">
      <xsl:copy>
        <xsl:apply-templates select="@* | node()" />
      </xsl:copy>
    </xsl:if>
  </xsl:template>

  <!-- Add a Cwm prefix to the name of each generated Java class.  -->
  <xsl:template
    match="Model:Package/Model:Namespace.contents/Model:Class">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <Model:Namespace.contents>
        <Model:Tag tagId='javax.jmi.substituteName'>
          <xsl:attribute name="elements">
            <xsl:value-of select="@xmi.id"/>
          </xsl:attribute>
          <Model:Tag.values>
            <xsl:value-of select="concat('Cwm',@name)"/>
          </Model:Tag.values>
        </Model:Tag>
      </Model:Namespace.contents>
    </xsl:copy>
  </xsl:template>

  <!-- Rename Java keyword default to defaultValue -->
  <xsl:template match="Model:Attribute[@name='default']">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <Model:Namespace.contents>
        <Model:Tag tagId='javax.jmi.substituteName'>
          <xsl:attribute name="elements">
            <xsl:value-of select="@xmi.id"/>
          </xsl:attribute>
          <Model:Tag.values>defaultValue</Model:Tag.values>
        </Model:Tag>
      </Model:Namespace.contents>
    </xsl:copy>
  </xsl:template>

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
