<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input an XMI document containing the extension model -->
<!-- and combines it to the Farrago model.  -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="omg.org/mof.Model/1.3"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- location of Farrago extension model XMI from ant -->
  <xsl:param name="extXmiFilename"/>

  <xsl:param name="pkgName" />

  <xsl:param name="pkgPrefix" />

  <!-- Insert the external model as peer to Farrago package -->
  <xsl:template match="XMI/XMI.content">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <Model:Package xmi.id='z3' 
        annotation='Top-level package' isRoot='false' isLeaf='false' 
        isAbstract='false' visibility='public_vis'>
        <xsl:attribute name="name">
          <xsl:value-of select="$pkgName"/>
        </xsl:attribute>
        <Model:Namespace.contents>
          <Model:Tag tagId='javax.jmi.packagePrefix' elements='z3'>
            <Model:Tag.values><xsl:value-of select="$pkgPrefix"/></Model:Tag.values>
          </Model:Tag>
          <Model:Tag tagId='org.netbeans.implPackagePrefix' elements='z3'>
            <Model:Tag.values><xsl:value-of select="$pkgPrefix"/></Model:Tag.values>
          </Model:Tag>
          <xsl:copy-of select="document($extXmiFilename)/XMI/XMI.content/*"/>
        </Model:Namespace.contents>
      </Model:Package>
    </xsl:copy>
  </xsl:template>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
