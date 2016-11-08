Global Data Hub
===============

Welcome to the Global Data hub repository.

What technologies?
------------------

'Global Data Hub' makes use of a few different technologies to detect changes in the source systems and apply those changes to the 'Enstar Conformed Schema'. There are a few distinct stages in the process:

    1. Changes to the source systems are detected by Attunity Replicate and ingested into HDFS.
    2. Changes are applied to a 'Replicated sources' copy of the source system.
    3. Changes in the replicated source are conformed to the Enstar Schema
    4. The changes are added to the Global Data hub for users to query.

Stages 2 - 4 are implemented in SparkSQL; These are wrapped in Oozie workflows and co-ordinators to assist scheduling.


Directory Setup 
----------------
  - **deployment**
    - **scripts** :  Deployment scripts 
  - **development** : scripts for setting up development environment
  	-  **docker** : docker files and their dependencies
  - **environent**
    - **avro**
        - **schemas** : Avro schema files
    - **hive**
        - **schemas** : hive table schemas
        - **queries** : transformation queries
    - **oozie** 
        - **coordinators** : Oozie co-ordinator XML files
        - **properties** :  Oozie job properties
        - **workflows** : Oozie workflow XML files
    - **spark**
        - **properties** : spark properties files
  - **source**
    - **GlobalDatahub** : Spark job sources
    - **UDFs** : Hive user defined functions sources.

Branching model
----------------
Git branching model based on: [a successful git branching model](http://nvie.com/posts/a-successful-git-branching-model/)

- `master` contains production code only. Should only be merged into when hotfixes or releases are complete
- `hotfix/hotfixname` to be used to fix any production issues with new minor versions
- `develop` to be used in development, always branch from the develop branch to create features
    - `feature/{featurename}` development of features. Should be merged into `develop`upon completion 

Developing
----------------

#### Branching strategy

Most development will be done on feature branches branched off `develop`, the following instructions should be used to create features:

		git clone https://enstargroup.visualstudio.com/_git/GlobalDataHub
		git checkout -b feature/{your feature name} develop
		git commit -am "Implemented {feature name}"
		git push origin feature/{your feature name}
Finally, open a pull request on github to merge your feature into `develop`

Note:
> When development of a new release or project begins you should make sure to update all maven release numbers relating to the new release.  

>Coding styles are enforced during maven builds, so you may notice minor changes to formating after running a build.


#### Local Development Environment

Run the createContainers.sh script under `development/docker`  this will create and launch two docker containers for local development.  The containers should restart with your machine so this will only need to be completed once.

you can open a terminal session on either container using:
```docker attach <containername>``` where containername either **attunity** or **quickstart.cloudera**. Exit containers using `ctrl+p` and `ctrl+q`, any other method will stop the container.

The two containers created are: 

- **attunity**: a local attunity server this can be accessed from:
[https://localhost:3552/attunityreplicate/5.0.2.25/#/tasks](), to start the attunity server run the following commands

```
	docker attach attunity
	/opt/attunity/replicate/bin/arep.ctl start
	press <ctrl+p> and <ctrl+q>
```
- **quickstart.cloudera**: a local cloudera instance.  The following ports are exposed on your local machine:
	- **80**: cloudera tutorials
	- **7180**: Cloudera manager
	- **8020**: HDFS
	- **8088**: Resource manager
	- **8888**: Hue
	- **10000**: Hive
	- **50111**: WebHcat
	- **21050**: Impala

Webhcat is disabled by default.  If you require this, please run the following commands:

```
	docker attach quickstart.cloudera
	python /root/servicesetup.py
	press <ctrl+p> and <ctrl+q>
```

Hosts
-----
```
UKGS2ATU01: Attunity.
UKGS2ATU02: Attunity.
UKGS2JENK01: Jenkins.

LSGNPDHGN01: Gateway Node.
LSGNPDHMN01: Master Node
LSGNPDHMN02: Master Node
LSGNPHDN01: Data node.
LSGNPHDN02: Data node.
LSGNPHDN03: Data node.
LSGNPHDN04: Data node.
```
#### Non Prod
```
UKNPDATU01: Attunity.
UKNPDJENK01: Jenkins.
UKNPDNEX01: Nexus.
UKNPDCNAV01: Cloudera Navigator.
```
 

Service Accounts
----------------
- **gdhcdcprocessor**: Runs the job to apply change data capture change table entries to source table copies
- **gdhtransformprocessor**: Runs the job to transform source table copies to conformed data model.
- **gdhetlprocessor**: Runs other ETL process jobs.
- **gdhpublishprocessor**: Runs the job to load the transformed data into the conformed data model
		