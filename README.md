**Marketing Analysis case study**

This git-repository contains **three** branches:
- _master_      the basic implementation
- _feature/alternative-implementation_ the alternative implementation
- _feature/alternative-aggregator-on-sorted-set_    another alternative implementation, with aggregator based on 
SortedSet instead of List

**Change notes on**     _feature/alternative-implementation_ 
- alternative DataFrame's implementation for the Tasks 2.1 and 2.2
- more aggressive refactoring
- added application configuration
- output to the filesystem, configurable
- top companies N as a parameter in application.conf
- could be run as assembly:

            sbt assembly
            
            java -jar target/scala-2.12/SparkTest-assembly-1.0.jar eventsInput--/path/to/file.csv purchasesInput--/path/to/file2.csv 
        

**General notes**
- in the sake of performance, some intermediate data stored in temporary tables
- in branch _feature/alternative-aggregator-on-sorted-set_ SortedSet is used as a container for the Sessions,
it allows to get rid of re-sorting entire collection while reduce and merge phases 

**Tests**
        _sbt test_
        
**Aggregator task implementation details**

The main idea is that if we dont have order inside each group, but we know that event 
with type=app_open definitely starts new session. Hence on reduce phase we can create list
of sessions plus list of unspecified events.

On the merge phase we can insert raw events into existing sessions, the only thing we need to do
is to choose correct session. Session list ordered by session's start time

