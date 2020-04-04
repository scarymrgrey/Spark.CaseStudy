**Marketing Analysis case study**

In this git-repository contains **two** branches:
- _master_      basic implementation
- _feature/alternative-implementation_ alternative implementation

**Change notes on**     _feature/alternative-implementation_ 
- alternative DataFrame's implementation for the Tasks 1.1 and 1.2
- more aggressive refactoring
- added application configuration

**Aggregator task implementation details**

The main idea is that if we dont have order inside each group, but we know that event 
with type=app_open definitely starts new session. Hence on reduce phase we can create List
of sessions plus list of unspecified events.

On the merge phase we can insert raw events into existing sessions, the only thing we need to do
is to choose correct session. Session list ordered by session start time

