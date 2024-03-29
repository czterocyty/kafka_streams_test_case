# Test case for TopologyTestDriver out of order output records

[Kafka Stream discussion thread](https://lists.apache.org/thread.html/73bd0dc0dbdbad237757425c7bd3b6e721083fbd46c2bab29618ddd9@%3Cdev.kafka.apache.org%3E)

Implemented in Kotlin, JVM 11 and against Kafka Streams 2.3.

Buildable by Gradle (wrapper).

Typical test session on Ubuntu:

```bash
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 ./gradlew clean test
```

## Topology

```
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [child])
      --> KSTREAM-TRANSFORMVALUES-0000000003
    Processor: KSTREAM-TRANSFORMVALUES-0000000003 (stores: [mapping])
      --> KSTREAM-BRANCH-0000000004
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-BRANCH-0000000004 (stores: [])
      --> KSTREAM-BRANCHCHILD-0000000005, KSTREAM-BRANCHCHILD-0000000006
      <-- KSTREAM-TRANSFORMVALUES-0000000003
    Processor: KSTREAM-BRANCHCHILD-0000000006 (stores: [])
      --> KSTREAM-FILTER-0000000009
      <-- KSTREAM-BRANCH-0000000004
    Processor: KSTREAM-FILTER-0000000009 (stores: [])
      --> KSTREAM-MAPVALUES-0000000010
      <-- KSTREAM-BRANCHCHILD-0000000006
    Processor: KSTREAM-MAPVALUES-0000000010 (stores: [])
      --> KSTREAM-MERGE-0000000017, KSTREAM-FLATMAP-0000000015
      <-- KSTREAM-FILTER-0000000009
    Source: KSTREAM-SOURCE-0000000001 (topics: [poison_pill])
      --> KSTREAM-TRANSFORMVALUES-0000000002
    Source: KSTREAM-SOURCE-0000000013 (topics: [routed])
      --> KSTREAM-MAPVALUES-0000000014
    Processor: KSTREAM-TRANSFORMVALUES-0000000002 (stores: [mapping])
      --> KSTREAM-MERGE-0000000017
      <-- KSTREAM-SOURCE-0000000001
    Processor: KSTREAM-MAPVALUES-0000000014 (stores: [])
      --> KSTREAM-MERGE-0000000018
      <-- KSTREAM-SOURCE-0000000013
    Processor: KSTREAM-MERGE-0000000017 (stores: [])
      --> KSTREAM-MERGE-0000000018
      <-- KSTREAM-MAPVALUES-0000000010, KSTREAM-TRANSFORMVALUES-0000000002
    Processor: KSTREAM-MERGE-0000000018 (stores: [])
      --> KSTREAM-AGGREGATE-0000000019
      <-- KSTREAM-MERGE-0000000017, KSTREAM-MAPVALUES-0000000014
    Processor: KSTREAM-AGGREGATE-0000000019 (stores: [parents])
      --> KTABLE-TOSTREAM-0000000020
      <-- KSTREAM-MERGE-0000000018
    Processor: KTABLE-TOSTREAM-0000000020 (stores: [])
      --> KSTREAM-BRANCH-0000000021
      <-- KSTREAM-AGGREGATE-0000000019
    Processor: KSTREAM-BRANCH-0000000021 (stores: [])
      --> KSTREAM-BRANCHCHILD-0000000022, KSTREAM-BRANCHCHILD-0000000023
      <-- KTABLE-TOSTREAM-0000000020
    Processor: KSTREAM-BRANCHCHILD-0000000022 (stores: [])
      --> KSTREAM-FILTER-0000000024
      <-- KSTREAM-BRANCH-0000000021
    Processor: KSTREAM-FILTER-0000000024 (stores: [])
      --> KSTREAM-MAPVALUES-0000000025
      <-- KSTREAM-BRANCHCHILD-0000000022
    Processor: KSTREAM-BRANCHCHILD-0000000005 (stores: [])
      --> KSTREAM-FILTER-0000000007
      <-- KSTREAM-BRANCH-0000000004
    Processor: KSTREAM-BRANCHCHILD-0000000023 (stores: [])
      --> KSTREAM-FILTER-0000000026
      <-- KSTREAM-BRANCH-0000000021
    Processor: KSTREAM-MAPVALUES-0000000025 (stores: [])
      --> KSTREAM-FLATMAP-0000000030, KSTREAM-MAP-0000000028
      <-- KSTREAM-FILTER-0000000024
    Processor: KSTREAM-FILTER-0000000007 (stores: [])
      --> KSTREAM-MAPVALUES-0000000008
      <-- KSTREAM-BRANCHCHILD-0000000005
    Processor: KSTREAM-FILTER-0000000026 (stores: [])
      --> KSTREAM-MAPVALUES-0000000027
      <-- KSTREAM-BRANCHCHILD-0000000023
    Processor: KSTREAM-MAPVALUES-0000000008 (stores: [])
      --> KSTREAM-MAP-0000000011
      <-- KSTREAM-FILTER-0000000007
    Processor: KSTREAM-MAPVALUES-0000000027 (stores: [])
      --> KSTREAM-MAP-0000000032
      <-- KSTREAM-FILTER-0000000026
    Processor: KSTREAM-FLATMAP-0000000015 (stores: [])
      --> KSTREAM-SINK-0000000016
      <-- KSTREAM-MAPVALUES-0000000010
    Processor: KSTREAM-FLATMAP-0000000030 (stores: [])
      --> KSTREAM-SINK-0000000031
      <-- KSTREAM-MAPVALUES-0000000025
    Processor: KSTREAM-MAP-0000000011 (stores: [])
      --> KSTREAM-SINK-0000000012
      <-- KSTREAM-MAPVALUES-0000000008
    Processor: KSTREAM-MAP-0000000028 (stores: [])
      --> KSTREAM-SINK-0000000029
      <-- KSTREAM-MAPVALUES-0000000025
    Processor: KSTREAM-MAP-0000000032 (stores: [])
      --> KSTREAM-SINK-0000000033
      <-- KSTREAM-MAPVALUES-0000000027
    Sink: KSTREAM-SINK-0000000012 (topic: routed)
      <-- KSTREAM-MAP-0000000011
    Sink: KSTREAM-SINK-0000000016 (topic: poison_pill)
      <-- KSTREAM-FLATMAP-0000000015
    Sink: KSTREAM-SINK-0000000029 (topic: parent)
      <-- KSTREAM-MAP-0000000028
    Sink: KSTREAM-SINK-0000000031 (topic: routed)
      <-- KSTREAM-FLATMAP-0000000030
    Sink: KSTREAM-SINK-0000000033 (topic: parent)
      <-- KSTREAM-MAP-0000000032
```

![topology](topology.png)
Image generated by [https://zz85.github.io/kafka-streams-viz/](https://zz85.github.io/kafka-streams-viz/)
