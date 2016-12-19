# Environments

This page lists the main environments used on this project and the nodes on each.

## Development/CI

Integration testing and development. Note that most development related testing should be carried out locally.

### URLs

*   [Cloudera Manager](https://UKNPDCNAV01:7180) (web console)
*   [HUE](https://LSGNPDHMN02:8888) (web console)
*   [YARN Resource manager](https://LSGNPDHMN01:8090/cluster) (web console)
*   [YARN Job History](https://LSGNPDHMN0219890/jobhistory) (web console)
*   [HDFS Name Node](https://LSGNPDHMN01:50470) (web console)
*   [Hive](https://LSGNPDHMN01:10002/hiveserver2.jsp) (web console)
*   [Spark history](https://LSGNPDHMN02:18088) (web console)
*   [OOZIE](https://LSGNPDHMN02:11443/oozie) (for job submission, use HUE to see Workflows/Coordinators)
*   [Jenkins](https://UKNPDJENK01) (web console)

### Common ports

**HDFS**: LSGNPDHMN01:8020

**Hive**: LSGNPDHMN01:10000

**Impala**:  LSGNPDHMN02:21050

**WebHCat**: LSGNPDHMN01:50111

### Nodes

*   **UKNPDATU01**: Attunity.
*   **UKNPDJENK01**: Jenkins.
*   **UKNPDNEX01**: Nexus.
*   **UKNPDCNAV01**: Cloudera Navigator.
*   **LSGNPDHGN01**: Gateway Node.
*   **LSGNPDHMN01**: Master Node
*   **LSGNPDHMN02**: Master Node
*   **LSGNPHDN01**: Data node.
*   **LSGNPHDN02**: Data node.
*   **LSGNPHDN03**: Data node.
*   **LSGNPHDN04**: Data node.

## Pre-production

## Production