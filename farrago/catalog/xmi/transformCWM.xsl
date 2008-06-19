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

  <!-- empty string if no extension model is present -->
  <xsl:param name="extPresent"/>

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
          <xsl:if test="$extPresent">
            <xsl:copy-of select="document($extXmiFilename)/XMI/XMI.content/*"/>
          </xsl:if>
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
  <!-- Also overrides the max length tag at the class level for    -->
  <!-- some special cases:                                         -->
  <!--   Type.attribute            Required Size                   -->
  <!--   Dependency.name           ModelElement.name + 4           -->
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
        <xsl:if test="@name = 'Dependency'">
          <Model:Tag tagId='org.eigenbase.enki.maxLength'>
            <xsl:attribute name="elements">
              <xsl:value-of select="@xmi.id"/>
            </xsl:attribute>
            <Model:Tag.values>132</Model:Tag.values>
          </Model:Tag>
        </xsl:if>
      </Model:Namespace.contents>
    </xsl:copy>
  </xsl:template>

  <!-- Apply max length tag to columns in CWM that require it: -->
  <!--   Type.attribute            Required Size               -->
  <!--   Expression.body           unlimited                   -->
  <!--   Sqlindex.filterCondition  unlimited                   -->
  <!--   DataValue.value           unlimited                   -->
  <!--   TaggedValue.value         unlimited                   -->
  <xsl:template match="Model:Class[@name='Expression']/Model:Namespace.contents/Model:Attribute[@name='body']
      | Model:Class[@name='SQLIndex']/Model:Namespace.contents/Model:Attribute[@name='filterCondition']
      | Model:Class[@name='DataValue']/Model:Namespace.contents/Model:Attribute[@name='value']
      | Model:Class[@name='TaggedValue']/Model:Namespace.contents/Model:Attribute[@name='value']">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <Model:Namespace.contents>
        <Model:Tag tagId='org.eigenbase.enki.maxLength'>
          <xsl:attribute name="elements">
            <xsl:value-of select="@xmi.id"/>
          </xsl:attribute>
          <Model:Tag.values>unlimited</Model:Tag.values>
        </Model:Tag>
      </Model:Namespace.contents>
    </xsl:copy>
  </xsl:template>

  <!-- Apply lazy association tag to some associations: -->
  <!--   ParameterType             -->
  <!--   StructuralFeatureType     -->
  <xsl:template match="Model:Association[@name='ParameterType']
      | Model:Association[@name='StructuralFeatureType']">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
      <Model:Namespace.contents>
        <Model:Tag tagId='org.eigenbase.enki.lazyAssociation'>
          <xsl:attribute name="elements">
            <xsl:value-of select="@xmi.id"/>
          </xsl:attribute>
          <Model:Tag.values>true</Model:Tag.values>
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

  <!-- Delete all package clustering -->
  <xsl:template match="@isClustered"/>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
