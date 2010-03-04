<?xml   version="1.0"   encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	version="1.0">

	<xsl:variable name="time" select="sum(//@duration) div 1000" />
	
	<xsl:variable name="failures" select="count(//test-result[@result='FAILURE'])" />
	
	<xsl:variable name="errorcount" select="count(//test-result[@result!='SUCCESS'])-$failures" />

	<xsl:template match="/">
		<testsuite>
			<xsl:attribute name="errors">
				<xsl:value-of select="$errorcount" />
			</xsl:attribute>

			<xsl:attribute name="failures">
				<xsl:value-of select="$failures" />
			</xsl:attribute>
			<xsl:attribute name="tests">
				<xsl:value-of select="//header-info/@resultcount" />
			</xsl:attribute>

			<xsl:attribute name="time">
				<xsl:value-of select="$time" />
			</xsl:attribute>

			<xsl:attribute name="timestamp">
				<xsl:value-of select="//header-info/@execdate" />
			</xsl:attribute>
			<xsl:for-each select="/test-log/test-result">
				<testcase>
					<xsl:choose>
						<xsl:when test="@result='SUCCESS'">
							<xsl:attribute name="classname">
								<xsl:value-of
									select="./test-case/@testcasename" />
							</xsl:attribute>
							<xsl:attribute name="name">
								<xsl:value-of
									select="./test-case/@basename" />
							</xsl:attribute>
							<xsl:attribute name="time">
								<xsl:value-of select="./@duration div 1000" />
							</xsl:attribute>

						</xsl:when>
						<xsl:otherwise>

							<xsl:attribute name="classname">
								<xsl:value-of
									select="./test-case/@testcasename" />
							</xsl:attribute>
							<xsl:attribute name="name">
								<xsl:value-of
									select="./test-case/@basename" />
							</xsl:attribute>
							<xsl:attribute name="time">
								<xsl:value-of select="./@duration div 1000" />
							</xsl:attribute>
							<failure>
								<xsl:attribute name="type">
									<xsl:value-of
										select="./execution-output/@errorname" />
								</xsl:attribute>
								<xsl:value-of
									select="./execution-output/output-details" />
							</failure>

						</xsl:otherwise>
					</xsl:choose>

				</testcase>

			</xsl:for-each>

		</testsuite>
	</xsl:template>
</xsl:stylesheet>
