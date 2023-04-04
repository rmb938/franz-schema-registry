# franz-schema-registry
Kafka Schema Registry that is API compatible with [Confluent Schema Registry](https://github.com/confluentinc/schema-registry)

## Why?

The [architecture](https://docs.confluent.io/platform/current/schema-registry/multidc.html#multi-datacenter-setup) of 
Confluent Schema Registry requires the deployment of a single read/write cluster that can be mirrored to many read-only 
replicas. In a normal architecture this means that your read/write cluster is in a single region. If this single region 
goes down there is no straight-forward way to promote a read-only replica to read/write. The promotion process requires 
all the mirrors to be modified, configuration changes and possible re-deployments of clusters. This takes time, is 
error-prone, and cannot easily be automated.

One alternative is to use [Multi-Cluster Schema Registry](https://docs.confluent.io/platform/current/control-center/topics/schema.html#enabling-multi-cluster-sr)
This however requires the use of Confluent Control Center which is a paid product. This also just distributes schemas 
across multiple registries and does not necessarily make disaster recovery easier to implement. This can also segregate
schemas to specific Kafka clusters and goes against [best practice](https://www.confluent.io/blog/17-ways-to-mess-up-self-managed-schema-registry/) 
of having a single globally available schema registry.

Another solution is [Schema Linking](https://docs.confluent.io/platform/current/schema-registry/schema-linking-cp.html#what-is-schema-linking)
But this also requires a paid product and further complicates the architecture of Schema Registry.

Franz Schema Registry takes a different approach. Instead of relying on Kafka for its data storage it uses
[Google Spanner](https://cloud.google.com/spanner), specifically the [Postgres Interface](https://cloud.google.com/spanner/docs/postgresql-interface)
Google Spanner is a SQL-based multi-regional distributed database. It removes the reliance on a single region and removes the 
complicated mirroring and recovery architecture from Schema Registry.

### Pros & Cons

#### Pros

* Simplified Architecture
* Faster and Easier Disaster Recovery; handled mostly by Google Spanner itself
* Globally Available Database & Clustering

#### Cons

* Possibly Incompatibility with [Schema Validation on Confluent Server](https://docs.confluent.io/platform/current/schema-registry/schema-validation.html)
* Requires the use of another technology; Google Spanner

## Production Deployment

Franz Schema Registry is not production ready and is not recommended to be deployed.

## Development

1. Clone this repo
2. Spin up a local postgres database via `docker-compose up`
3. Run the application via `make run`

## Features Implemented

- [X] Avro Schemas
  - [X] Loading & Validating 
  - [X] Backwards Compatibility
- [ ] Protobuf Schemas
  - [ ] Loading & Validating
  - [ ] Backwards Compatibility
- [ ] JSON Schemas
  - Some code is written to load schemas & check backward compatibility, but it's complicated and there is a lot of nuance
  - Until I find a library to do this or there is official guidance from JSON Schema Org this won't be supported for now
  - If you are a JSON Schema expert feel free to contribute
  - [ ] Loading & Validating
  - [ ] Backwards Compatibility
- [X] Schema References
- [X] Schema Compatibility Checks
- [ ] Schema Normalization - https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#schema-normalization
- [ ] Prometheus Metrics
- [ ] ACLs
- [ ] Full `/schemas` API compatibility
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id-schema
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-types-
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id-versions
  - [ ] Unit Testing
  - [ ] e2e Testing
- [ ] Full `/subjects` API compatibility
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)-schema
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)
  - [X] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-versionId-%20version-referencedby
  - [X] Unit Testing
  - [ ] e2e Testing
- [ ] Full `/mode` API compatibility
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--mode
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#put--mode
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--mode-(string-%20subject)
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#put--mode-(string-%20subject)
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--mode-(string-%20subject)
  - [ ] Unit & e2e Testing
- [ ] Full `/compatibility` API compatibility
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--compatibility-subjects-(string-%20subject)-versions-(versionId-%20version)
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--compatibility-subjects-(string-%20subject)-versions
  - [ ] Unit Testing
  - [ ] e2e Testing
- [ ] Full `/config` API compatibility
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#put--config
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--config
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#put--config-(string-%20subject)
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--config-(string-%20subject)
  - [ ] https://docs.confluent.io/platform/current/schema-registry/develop/api.html#delete--config-(string-%20subject)
  - [ ] Unit Testing
  - [ ] e2e Testing
- [ ] Full `/exporters` API compatibility
  - This most likely will not be implemented
