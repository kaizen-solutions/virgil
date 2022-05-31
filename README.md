# Virgil
_Virgil is a functional Cassandra client built using ZIO, Magnolia and the Datastax 4.x Java drivers_

![Build Status](https://github.com/kaizen-solutions/virgil/actions/workflows/ci.yml/badge.svg)

[![Latest Version](https://jitpack.io/v/kaizen-solutions/virgil.svg)](https://jitpack.io/#kaizen-solutions/virgil)

[![Coverage Status](https://coveralls.io/repos/github/kaizen-solutions/virgil/badge.svg)](https://coveralls.io/github/kaizen-solutions/virgil)

[![Chat on Discord](https://img.shields.io/discord/955126399100932106?label=chat%20on%20discord)](https://discord.gg/8EvqF9VPrp)

[Javadoc](https://javadoc.jitpack.io/com/github/kaizen-solutions/virgil/virgil_2.13/0.5.0/javadoc/io/kaizensolutions/virgil/index.html)


## Quick Start

Add the JitPack resolver and import Virgil (this will transitively import the Datastax Java Driver, Magnolia and ZIO 1.x):
```sbt
resolvers           += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.kaizen-solutions.virgil" %% "virgil" % "<please-see-jitpack-badge-for-latest-version>"
```

Please note that Virgil is built for Scala 2.12.x, 2.13.x and 3.1.x but fully-automatic derivation is not present for 3.1.x.
Virgil also supports both **ZIO 1.x** and **ZIO 2.x** (select an artifact with the `zio2` suffix).

## Introduction

You can follow along by checking out this repository and running `docker-compose up` which will bring up 
Datastax Enterprise Cassandra node along with Datastax Studio which provides a nice UI to interact with Cassandra. You 
have to create a new connection in Datastax Studio where you point to the container (since this is running in the same 
Docker network, we utilize the Docker DNS to resolve the hostname to the container IP and the hostname of the Cassandra 
cluster is `datastax-enterprise`):

<img src="https://user-images.githubusercontent.com/14280155/154575887-d568cd9e-54d8-4354-8c3f-11a417b63d40.png" width="400">
<br>
<img src="https://user-images.githubusercontent.com/14280155/154576357-7966f124-2f3c-4195-a746-412b3bbe257f.png" width="400">
<br>
<img src="https://user-images.githubusercontent.com/14280155/154576610-4f49fe72-9461-4c1f-873b-c3b2b93f1d32.png" width="400">

Please keep reading if you want to follow along 👇

### Keyspace setup

Given the following Cassandra keyspace:
```cql
CREATE KEYSPACE IF NOT EXISTS virgil
  WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
```

### Table setup

And the following Casandra table along with its User Defined Types (UDTs) (Make sure we are using the keyspace with `USE virgil`):
```cql
CREATE TYPE info (
  favorite BOOLEAN,
  comment TEXT
);

CREATE TYPE address (
  street TEXT,
  city TEXT,
  state TEXT,
  zip INT,
  data frozen<list<info>>
);

CREATE TABLE IF NOT EXISTS persons (
  id TEXT,
  name TEXT,
  age INT,
  past_addresses frozen<set<address>>,
  PRIMARY KEY ((id), age)
);
```

### Scala data-types 

If we want to read and write data to this table, we create case classes that mirror the table and UDTs in Scala:

```scala
import io.kaizensolutions.virgil.annotations.CqlColumn

final case class Info(favorite: Boolean, comment: String)
final case class Address(street: String, city: String, state: String, zip: Int, data: List[Info])
final case class Person(
  id: String, 
  name: String, 
  age: Int, 
  @CqlColumn("past_addresses") addresses: Set[Address]
)
```

Note that the `CqlColumn` annotation can be used if the column/field name in the Cassandra table is different from the 
Scala representation. This can also be used inside User Defined Types as well.

### Scala 3 caveats

If you are using Scala 3.1.x, you will need to use semi-automatic derivation as I have not yet figured out how to enable 
fully automatic derivation like Scala 2.x has.

```scala3
final case class Info(favorite: Boolean, comment: String)
object Info:
    given cqlUdtValueEncoderForInfo: CqlUdtValueEncoder.Object[Info] = CqlUdtValueEncoder.derive[Info]
    given cqlUdtValueDecoderForInfo: CqlUdtValueDecoder.Object[Info] = CqlUdtValueDecoder.derive[Info]

final case class Address(street: String, city: String, state: String, zip: Int, data: List[Info])
object Address:
    given cqlUdtValueEncoderForAddress: CqlUdtValueEncoder.Object[Address] = CqlUdtValueEncoder.derive[Address]
    given cqlUdtValueDecoderForAddress: CqlUdtValueDecoder.Object[Address] = CqlUdtValueDecoder.derive[Address]

final case class Person(
    id: String,
    name: String,
    age: Int,
    @CqlColumn("past_addresses") addresses: Set[Address]
)
object Person:
    given cqlRowDecoderForPersonForPerson: CqlRowDecoder.Object[Person] = CqlRowDecoder.derive[Person]
```

### Writing data

Now that all the data-types are in place, we can write some data:
```scala
import io.kaizensolutions.virgil._
import io.kaizensolutions.virgil.dsl._

def insert(p: Person): CQL[MutationResult] =
  InsertBuilder("persons")
    .value("id", p.id)
    .value("name", p.name)
    .value("age", p.age)
    .value("past_addresses", p.addresses)
    .build

def setAddress(personId: String, personAge: Int, address: Address): CQL[MutationResult] =
  UpdateBuilder("persons")
    .set("past_addresses" := Set(address))
    .where("id" === personId)
    .and("age" === personAge)
    .build
```

We can also read data:
```scala
def select(personId: String, personAge: Int): CQL[Person] =
  SelectBuilder
    .from("persons")
    .columns("id", "name", "age", "past_addresses")
    .where("id" === personId)
    .and("age" === personAge)
    .build[Person]
    .take(1)
```

## Low level API

If you find that you have a complex query that cannot be expressed with the DSL yet, then you can use the lower level cql 
interpolator to express your query or mutation:
```scala
import io.kaizensolutions.virgil.cql._
def selectAll: CQL[Person] =
  cql"SELECT id, name, age, addresses FROM persons".query[Person]
  
def insertLowLevel(p: Person): CQL[MutationResult] =
  cql"INSERT INTO persons (id, name, age, addresses) VALUES (${p.id}, ${p.name}, ${p.age}, ${p.addresses}) USING TTL 10".mutation
```

Note that the lower-level API will turn the CQL into a string along with bind markers for each parameter and use bound
statements under the hood, so you do not have to worry about CQL injection attacks. 

If you want to string interpolate some part of the query because you may not know your table name up front (i.e. its
passed through configuration, then you can use `s"I am a String ${forExample}".appendCql(cql"continuing the cassandra query")` 
or `cql"SELECT * FROM ".appendString(s"$myTable")`). Doing interpolation in cql is different from string interpolation
as it will cause bind markers to be created.

### Cassandra batches

You can also batch (i.e. Cassandra's definition of the word) mutations together by using `+`:
```scala
val batch: CQL[MutationResult]         = insert(p1) + update(p2.id, newPInfo) + insert(p3)
val unloggedBatch: CQL[MutationResult] = CQL.unlogged(batch)
```

Note: You cannot batch together queries and mutations as this is not allowed by Cassandra.

### Compiling CQL queries and mutations into streaming effects

Now that we have built our CQL queries and mutations, we can execute them:
```scala
import zio._
import zio.stream._

// A single element stream is returned
val insertResult: ZStream[Has[CQLExecutor], Throwable, MutationResult] = insert(person).execute

// A stream of results is returned
val queryResult: ZStream[Has[CQLExecutor], Throwable, Person] = selectAll.execute
```

### Executing queries and mutations

Running CQL queries and mutations is done through the `CQLExecutor`, which produces a `ZStream` that contains the 
results. You can obtain a `CQLExecutor` layer provided you have a `CqlSessionBuilder` from the Datastax Java Driver:
```scala
val dependencies: ULayer[Has[CQLExecutor]] = {
  val cqlSessionBuilderLayer: ULayer[Has[CqlSessionBuilder]] =
    ZLayer.succeed(
      CqlSession
        .builder()
        .withKeyspace("virgil")
        .withLocalDatacenter("dc1")
        .addContactPoint(InetSocketAddress.createUnresolved("localhost", 9042))
        .withApplicationName("virgil-tester")
  )
  val executor: ZLayer[Any, Throwable, Has[CQLExecutor]] = cqlSessionBuilderLayer >>> CQLExecutor.live
  executor.orDie
}

val insertResultReady: Stream[Throwable, MutationResult] = insertResult.provideLayer(dependencies)
```

### Adding support for custom data types
Virgil provides all the default primitive data-types supported by the Datastax Java Driver. However, you can add support 
for your own primitive data-types. For example, if you want to add support for `java.time.LocalDateTime`, you can do so 
in the following manner (make sure to be extra careful when managing timezones): 

```scala
import io.kaizensolutions.virgil.codecs._
import java.time.{LocalDateTime, ZoneOffset}

implicit val javaTimeInstantEncoder: CqlPrimitiveEncoder[LocalDateTime] =
  CqlPrimitiveEncoder[java.time.Instant].contramap[java.time.LocalDateTime](_.toInstant(ZoneOffset.UTC))

implicit val javaTimeInstantDecoder: CqlPrimitiveDecoder[LocalDateTime] =
  CqlPrimitiveDecoder[java.time.Instant].map[LocalDateTime](instant =>
    LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
  )
```

### Why the name Virgil?
Virgil was an ancient Roman poet who composed an epic poem about Cassandra and so we thought it would be appropriate.

### Inspiration
We were heavily inspired by Doobie, Cassandra4IO and Quill and wanted a more native ZIO solution for Cassandra focused
on ergonomics, ease of use and performance (compile-time and runtime). Special thanks to John De Goes, Francis Toth and
Nigel Benns for their help, mentorship, and guidance.
