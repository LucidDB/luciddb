<?xml version='1.0' encoding='utf-8'?>
<!DOCTYPE moodssConfiguration>

<moodssConfiguration>
  <version>19.7</version>
  <date>09/24/06</date>
  <time>13:19:40</time>
  <configuration graphPlotBackground="black" currentValueTableRows="1000" canvasWidth="1280" canvasBackground="white" canvasImagePosition="nw" pieLabeler="peripheral" graphNumberOfIntervals="200" cellsLabelModuleHeader="1" graphXAxisLabelsRotation="90" graphLabelsPosition="right" canvasHeight="1024" graphDisplayGrid="0" canvasImageFile="" graphMinimumY="">
    <viewerColors>
      <item>#7FFFFF</item>
      <item>#7FFF7F</item>
      <item>#FF7F7F</item>
      <item>#FFFF7F</item>
      <item>#7F7FFF</item>
      <item>#FFBF00</item>
      <item>#BFBFBF</item>
      <item>#FF7FFF</item>
      <item>#FFFFFF</item>
    </viewerColors>
  </configuration>
  <width>1145</width>
  <height>725</height>
  <pollTime>1</pollTime>
  <modules>
    <module namespace="fennel&lt;0&gt;">
      <arguments>--file /tmp/fennel.stats</arguments>
      <tables>
        <table level="38" width="306" x="805.0" xIcon="2.0" height="220" y="78.0" yIcon="985.0"/>
      </tables>
    </module>
  </modules>
  <viewers>
    <viewer class="::thresholds">
      <cells/>
    </viewer>
    <viewer class="::store">
      <cells/>
    </viewer>
    <viewer class="::thresholdLabel">
      <cells/>
    </viewer>
    <viewer level="81" width="1123" x="0.0" height="266" y="233.0" class="::dataGraph">
      <cells>
        <item>fennel&lt;0&gt;::data(4,1)</item>
        <item>fennel&lt;0&gt;::data(6,1)</item>
        <item>fennel&lt;0&gt;::data(7,1)</item>
        <item>fennel&lt;0&gt;::data(9,1)</item>
        <item>fennel&lt;0&gt;::data(11,1)</item>
        <item>fennel&lt;0&gt;::data(12,1)</item>
        <item>fennel&lt;0&gt;::data(13,1)</item>
        <item>fennel&lt;0&gt;::data(14,1)</item>
        <item>fennel&lt;0&gt;::data(15,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#7F7FFF</item>
          <item>#BFBFBF</item>
          <item>#FF7FFF</item>
          <item>#7FFFFF</item>
          <item>#FF7F7F</item>
          <item>#FFFF7F</item>
          <item>#7F7FFF</item>
          <item>#FFBF00</item>
          <item>#BFBFBF</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="59" width="1123" x="0.0" height="183" y="23.0" class="::dataGraph">
      <cells>
        <item>fennel&lt;0&gt;::data(0,1)</item>
        <item>fennel&lt;0&gt;::data(2,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#7FFFFF</item>
          <item>#FF7F7F</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="68" width="574" x="549.0" height="177" y="528.0" class="::dataGraph">
      <cells>
        <item>fennel&lt;0&gt;::data(1,1)</item>
        <item>fennel&lt;0&gt;::data(3,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#7FFF7F</item>
          <item>#FFFF7F</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="67" width="549" x="0.0" height="177" y="528.0" class="::dataGraph">
      <cells>
        <item>fennel&lt;0&gt;::data(5,1)</item>
        <item>fennel&lt;0&gt;::data(8,1)</item>
        <item>fennel&lt;0&gt;::data(10,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#FFBF00</item>
          <item>#FFFFFF</item>
          <item>#7FFF7F</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="61" width="1123" x="0.0" height="23" y="0.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>High-volume instantaneous counters</endtext>
      </configuration>
    </viewer>
    <viewer level="75" width="1123" x="0.0" height="27" y="206.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>Low-volume instantaneous counters</endtext>
      </configuration>
    </viewer>
    <viewer level="80" width="549" x="0.0" height="29" y="499.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>Low-volume historical counters</endtext>
      </configuration>
    </viewer>
    <viewer level="77" width="574" x="549.0" height="29" y="499.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>High-volume historical counters</endtext>
      </configuration>
    </viewer>
  </viewers>
  <images/>
</moodssConfiguration>
