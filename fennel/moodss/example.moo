<?xml version='1.0'?>
<!DOCTYPE moodssConfiguration>

<moodssConfiguration>
  <version>17.1</version>
  <date>12/08/03</date>
  <time>21:15:30</time>
  <configuration graphNumberOfIntervals="200" canvasBackground="white" canvasWidth="1280" pieLabeler="peripheral" canvasHeight="1024" graphMinimumY="">
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
  <width>1152</width>
  <height>676</height>
  <pollTime>1</pollTime>
  <modules>
    <module namespace="fennel">
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
      <configuration>
        <comments/>
      </configuration>
    </viewer>
    <viewer class="::thresholdLabel">
      <cells/>
      <configuration>
        <text>Message:</text>
      </configuration>
    </viewer>
    <viewer level="37" width="1123" x="6.0" height="293" y="226.0" class="::dataGraph">
      <cells>
        <item>fennel::data(3,1)</item>
        <item>fennel::data(4,1)</item>
        <item>fennel::data(5,1)</item>
        <item>fennel::data(2,1)</item>
        <item>fennel::data(6,1)</item>
      </cells>
    </viewer>
    <viewer level="20" width="1121" x="6.0" height="206" y="8.0" class="::dataGraph">
      <cells>
        <item>fennel::data(0,1)</item>
        <item>fennel::data(1,1)</item>
      </cells>
    </viewer>
  </viewers>
</moodssConfiguration>
