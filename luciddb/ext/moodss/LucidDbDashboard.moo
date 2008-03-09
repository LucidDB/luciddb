<?xml version='1.0' encoding='utf-8'?>
<!DOCTYPE moodssConfiguration>

<moodssConfiguration>
  <version>19.7</version>
  <date>02/04/08</date>
  <time>09:12:14</time>
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
  <width>1137</width>
  <height>787</height>
  <pollTime>1</pollTime>
  <modules>
    <module namespace="luciddb&lt;0&gt;">
      <arguments>--file LucidDbPerfCounters.txt</arguments>
      <tables>
        <table level="112" width="306" x="575.0" xIcon="17.0" height="594" y="206.0" yIcon="985.0">
          <configuration toprow="2"/>
        </table>
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
    <viewer level="104" width="574" x="549.0" height="177" y="456.0" class="::dataGraph">
      <cells>
        <item>luciddb&lt;0&gt;::data(18446744073353902660,1)</item>
        <item>luciddb&lt;0&gt;::data(736717472,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#FFBF00</item>
          <item>#FF7FFF</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="103" width="549" x="0.0" height="177" y="456.0" class="::dataGraph">
      <cells>
        <item>luciddb&lt;0&gt;::data(18446744071617397978,1)</item>
        <item>luciddb&lt;0&gt;::data(374652009,1)</item>
        <item>luciddb&lt;0&gt;::data(1531934709,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744073109041616,1)</item>
        <item>luciddb&lt;0&gt;::data(1052357712,1)</item>
        <item>luciddb&lt;0&gt;::data(1102852748,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744071951220030,1)</item>
        <item>luciddb&lt;0&gt;::data(36719590,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744072864213359,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#7FFF7F</item>
          <item>#FFBF00</item>
          <item>#7FFFFF</item>
          <item>#7FFFFF</item>
          <item>#7FFF7F</item>
          <item>#FF7F7F</item>
          <item>#FFFF7F</item>
          <item>#7F7FFF</item>
          <item>#FFBF00</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="61" width="1123" x="0.0" height="23" y="0.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>High-volume instantaneous counters</endtext>
      </configuration>
    </viewer>
    <viewer level="85" width="1123" x="0.0" height="27" y="206.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>Low-volume instantaneous counters</endtext>
      </configuration>
    </viewer>
    <viewer level="100" width="549" x="0.0" height="29" y="427.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>Low-volume historical counters</endtext>
      </configuration>
    </viewer>
    <viewer level="102" width="574" x="549.0" height="29" y="427.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>High-volume historical counters</endtext>
      </configuration>
    </viewer>
    <viewer level="107" width="1123" x="0.0" height="183" y="23.0" class="::dataGraph">
      <cells>
        <item>luciddb&lt;0&gt;::data(18446744072975705558,1)</item>
        <item>luciddb&lt;0&gt;::data(1955520746,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#7F7FFF</item>
          <item>#BFBFBF</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="99" width="1123" x="0.0" height="194" y="233.0" class="::dataGraph">
      <cells>
        <item>luciddb&lt;0&gt;::data(18446744072035698204,1)</item>
        <item>luciddb&lt;0&gt;::data(1342517421,1)</item>
        <item>luciddb&lt;0&gt;::data(596220689,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744071612612392,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744073218643608,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744071808773478,1)</item>
        <item>luciddb&lt;0&gt;::data(730317744,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744073157127109,1)</item>
        <item>luciddb&lt;0&gt;::data(1411321961,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#FFFF7F</item>
          <item>#BFBFBF</item>
          <item>#FF7FFF</item>
          <item>#FFFFFF</item>
          <item>#7FFFFF</item>
          <item>#FF7F7F</item>
          <item>#FFFF7F</item>
          <item>#7F7FFF</item>
          <item>#FFFFFF</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="110" width="1123" x="0.0" height="138" y="660.0" class="::dataGraph">
      <cells>
        <item>luciddb&lt;0&gt;::data(961471793,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744073036866613,1)</item>
        <item>luciddb&lt;0&gt;::data(18446744072377611548,1)</item>
      </cells>
      <configuration labelsposition="left" yminimumcell="" yminimum="" grid="0" ymaximumcell="" ymaximum="">
        <cellcolors>
          <item>#7FFFFF</item>
          <item>#7FFF7F</item>
          <item>#FF7F7F</item>
        </cellcolors>
      </configuration>
    </viewer>
    <viewer level="109" width="1123" x="0.0" height="27" y="633.0" class="::freeText">
      <cells/>
      <configuration>
        <endtext>Memory counters</endtext>
      </configuration>
    </viewer>
  </viewers>
  <images/>
</moodssConfiguration>
