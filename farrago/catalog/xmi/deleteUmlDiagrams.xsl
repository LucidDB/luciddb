<?xml version="1.0"?> 
<!-- $Id$ -->
<!-- This stylesheet deletes UML diagrams from an XMI file -->

<xsl:stylesheet 
  version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:UML="org.omg.xmi.namespace.UML"
  xmlns:UML2="org.omg.xmi.namespace.UML2"
  >
  <xsl:output method="xml" indent="yes" />

  <!-- Delete diagrams -->
  <xsl:template match="UML:Diagram" />

  <!-- Delete property -->
  <xsl:template match="UML:Property" />

  <!-- Delete Uml1SemanticModelBridge, whatever the heck that is -->
  <xsl:template match="UML:Uml1SemanticModelBridge" />

  <!-- UML2 trick -->
  <xsl:template match="UML2:TypedElement.type">
    <xsl:element name="UML:StructuralFeature.type">
      <xsl:apply-templates />
    </xsl:element>
  </xsl:template>

  <!-- Pass everything else through unchanged -->
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
