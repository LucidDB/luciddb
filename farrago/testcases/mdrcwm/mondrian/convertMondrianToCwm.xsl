<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet takes as input a Mondrian schema -->
<!-- definition and transforms it into an instance of the -->
<!-- CWM Olap metamodel.  -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:Model="omg.org/mof.Model/1.3"
  xmlns:CWMOLAP = "org.omg.xmi.namespace.CWMOLAP"
  >
  <xsl:output method="xml" indent="yes" />

  <xsl:template match="Schema">
    <XMI xmi.version = '1.2' xmlns:CWM = 'org.omg.xmi.namespace.CWM' 
      xmlns:CWMTFM = 'org.omg.xmi.namespace.CWMTFM'
      xmlns:CWMOLAP = 'org.omg.xmi.namespace.CWMOLAP'>
      <xsl:attribute name="timestamp">
        <xsl:value-of select="'TODO'"/>
      </xsl:attribute>
      <XMI.header>
        <XMI.documentation>
          <XMI.exporter>convertMondrianToCwm</XMI.exporter>
          <XMI.exporterVersion>0.1</XMI.exporterVersion>
        </XMI.documentation>
      </XMI.header>
      <XMI.content>
        <CWMOLAP:Schema>
          <xsl:call-template name="expandNameAndId"/>
          <CWMOLAP:Schema.dimension>
            <xsl:apply-templates select="./Dimension" />
          </CWMOLAP:Schema.dimension>
        </CWMOLAP:Schema>
      </XMI.content>
    </XMI>
  </xsl:template>

  <xsl:template match="Dimension">
    <CWMOLAP:Dimension isAbstract = 'false' isMeasure = 'false'>
      <xsl:attribute name="isTime">
        <xsl:choose>
          <xsl:when test="@type = 'TimeDimension'">true</xsl:when>
          <xsl:otherwise>false</xsl:otherwise>
        </xsl:choose>
      </xsl:attribute>
      <xsl:call-template name="expandNameAndId"/>
      <CWMOLAP:Dimension.hierarchy>
        <xsl:apply-templates select="./Hierarchy" />
      </CWMOLAP:Dimension.hierarchy>
      <CWMOLAP:Dimension.memberSelection>
        <xsl:for-each select=".//Level">
          <xsl:call-template name="levelAsMemberSelection"/>
        </xsl:for-each>
      </CWMOLAP:Dimension.memberSelection>
    </CWMOLAP:Dimension>
  </xsl:template>

  <xsl:template match="Hierarchy">
    <CWMOLAP:LevelBasedHierarchy isAbstract = 'false'>
      <xsl:attribute name="name">
        <xsl:choose>
          <xsl:when test="@name">
            <xsl:value-of select="@name"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="../@name"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:attribute>
      <xsl:call-template name="generateId"/>
      <CWMOLAP:LevelBasedHierarchy.hierarchyLevelAssociation>
        <xsl:for-each select="Level">
          <xsl:call-template name="levelInHierarchy"/>
        </xsl:for-each>
      </CWMOLAP:LevelBasedHierarchy.hierarchyLevelAssociation>
    </CWMOLAP:LevelBasedHierarchy>
  </xsl:template>

  <xsl:template name="levelAsMemberSelection">
    <CWMOLAP:Level isAbstract = 'false'>
      <xsl:call-template name="expandNameAndId"/>
      <CWMOLAP:Level.hierarchyLevelAssociation>
        <CWMOLAP:HierarchyLevelAssociation>
          <xsl:attribute name="xmi.idref">
            <xsl:value-of select="concat('HLA_',generate-id())"/>
          </xsl:attribute>
        </CWMOLAP:HierarchyLevelAssociation>
      </CWMOLAP:Level.hierarchyLevelAssociation>
    </CWMOLAP:Level>
  </xsl:template>

  <xsl:template name="levelInHierarchy">
    <CWMOLAP:HierarchyLevelAssociation>
      <xsl:attribute name="name">
        <xsl:value-of select="./@name"/>
      </xsl:attribute>
      <xsl:attribute name="xmi.id">
        <xsl:value-of select="concat('HLA_',generate-id())"/>
      </xsl:attribute>
      <CWMOLAP:HierarchyLevelAssociation.currentLevel>
        <CWMOLAP:Level>
          <xsl:call-template name="generateIdref"/>
        </CWMOLAP:Level>
      </CWMOLAP:HierarchyLevelAssociation.currentLevel>
    </CWMOLAP:HierarchyLevelAssociation>
  </xsl:template>

  <xsl:template name="expandNameAndId">
    <xsl:attribute name="name">
      <xsl:value-of select="@name"/>
    </xsl:attribute>
    <xsl:call-template name="generateId"/>
  </xsl:template>

  <xsl:template name="generateId">
    <xsl:attribute name="xmi.id">
      <xsl:value-of select="generate-id()"/>
    </xsl:attribute>
  </xsl:template>

  <xsl:template name="generateIdref">
    <xsl:attribute name="xmi.idref">
      <xsl:value-of select="generate-id()"/>
    </xsl:attribute>
  </xsl:template>

  <!-- Filter out everything else -->
  <xsl:template match="/ | @* | node()">
    <xsl:apply-templates select="@* | node()" />
  </xsl:template>

</xsl:stylesheet>
