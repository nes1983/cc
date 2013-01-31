require 'rubygems'
require 'nokogiri'

XSL = <<-EOXSL
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="ISO-8859-1"/>
  <xsl:param name="indent-increment" select="'   '"/>
 
  <xsl:template name="newline">
    <xsl:text disable-output-escaping="yes">
</xsl:text>
  </xsl:template>
 
  <xsl:template match="comment() | processing-instruction()">
    <xsl:param name="indent" select="''"/>
    <xsl:call-template name="newline"/>
    <xsl:value-of select="$indent"/>
    <xsl:copy />
  </xsl:template>
 
  <xsl:template match="text()">
    <xsl:param name="indent" select="''"/>
    <xsl:call-template name="newline"/>
    <xsl:value-of select="$indent"/>
    <xsl:value-of select="normalize-space(.)"/>
  </xsl:template>
 
  <xsl:template match="text()[normalize-space(.)='']"/>
 
  <xsl:template match="*">
    <xsl:param name="indent" select="''"/>
    <xsl:call-template name="newline"/>
    <xsl:value-of select="$indent"/>
      <xsl:choose>
       <xsl:when test="count(child::*) > 0">
        <xsl:copy>
         <xsl:copy-of select="@*"/>
         <xsl:apply-templates select="*|text()">
           <xsl:with-param name="indent" select="concat ($indent, $indent-increment)"/>
         </xsl:apply-templates>
         <xsl:call-template name="newline"/>
         <xsl:value-of select="$indent"/>
        </xsl:copy>
       </xsl:when>
       <xsl:otherwise>
        <xsl:copy-of select="."/>
       </xsl:otherwise>
     </xsl:choose>
  </xsl:template>
</xsl:stylesheet>
EOXSL

def mergeConfigurations(low_prio_xml, high_prio_xml)
	low_xml = Nokogiri::XML(File.open(low_prio_xml))
	high_xml = Nokogiri::XML(File.open(high_prio_xml))

	low_props = low_xml.at_xpath("//property")
	high_props = high_xml.at_xpath("//property")

	seen = Hash.new(0)
	high_props.each {|n| seen[n.xpath('.//name').to_xml] += 1}
	low_props.each {|n| n.unlink if (seen[n.xpath('.//name').to_xml] += 1) > 1}

	conf = high_xml.at_xpath("//configuration")
	conf.add_child(high_props)
	conf.add_child(low_props)

	xsl = Nokogiri::XSLT(XSL)
	File.open('core-site.xml','w') {|f| f << xsl.apply_to(high_xml).to_s}
end

mergeConfigurations("default-config.xml","site-config.xml")
