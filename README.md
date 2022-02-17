# Virgil
_Virgil is a functional Cassandra client built using ZIO, Magnolia and the Datastax 4.x Java drivers_

![Build Status](https://github.com/kaizen-solutions/virgil/actions/workflows/ci.yml/badge.svg)

[![Latest Version](https://jitpack.io/v/kaizen-solutions/virgil.svg)](https://jitpack.io/#kaizen-solutions/virgil)

[Documentation](https://javadoc.jitpack.io/com/github/kaizen-solutions/virgil/virgil_2.13/main-7a8b8ba88b-1/javadoc/io/kaizensolutions/virgil/index.html)

## Quick Start

Import Virgil (this will transitively import the Datastax Java Driver, Magnolia and ZIO 1.x):
```sbt
resolvers           += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.kaizen-solutions" % "virgil" % "<please-see-badge-for-latest-version>"
```

Given the following Cassandra keyspace:
```cql
CREATE KEYSPACE IF NOT EXISTS virgil
  WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
```

Make sure we are using the keyspace with `USE virgil` and the following Casandra table along with its User Defined Types (UDTs):
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
  addresses frozen<set<address>>,
  PRIMARY KEY ((id), age)
);
```

If we want to read and write data to this table, we create case classes that mirror the table and UDTs in Scala through 
along with adding codecs for each datatype:

```scala
import io.kaizensolutions.virgil.codecs._

final case class Info(favorite: Boolean, comment: String)

object Info {
  implicit val decoderInfo: UdtDecoder[Info] = ColumnDecoder.deriveUdtValue[Info]
  implicit val encoderInfo: UdtEncoder[Info] = ColumnEncoder.deriveUdtValue[Info]
}

final case class Address(street: String, city: String, state: String, zip: Int, data: List[Info])

object Address {
  implicit val decoderAddress: UdtDecoder[Address] = ColumnDecoder.deriveUdtValue[Address]
  implicit val encoderAddress: UdtEncoder[Address] = ColumnEncoder.deriveUdtValue[Address]
}

// Note: This is the top level row, we write out its components so we don't need an Encoder for the whole Person
final case class Person(id: String, name: String, age: Int, addresses: Set[Address])
object Person {
  implicit val decoderPerson: Decoder[Person] = Decoder.derive[Person]
}
```

Now that all the datatypes are in place, we can write some data:
```scala
import io.kaizensolutions.virgil._
import io.kaizensolutions.virgil.dsl._

def insert(p: Person): CQL[MutationResult] =
  InsertBuilder("persons")
    .value("id", p.id)
    .value("name", p.name)
    .value("age", p.age)
    .value("addresses", p.addresses)
    .build

def setAddress(personId: String, personAge: Int, address: Address): CQL[MutationResult] =
  UpdateBuilder("persons")
    .set("addresses" := Set(address))
    .where("id" === personId)
    .and("age" === personAge)
    .build
```

We can also read data:
```scala
def select(personId: String, personAge: Int): CQL[Person] =
  SelectBuilder
    .from("persons")
    .columns("id", "name", "age", "addresses")
    .where("id" === personId)
    .and("age" === personAge)
    .build[Person]
    .take(1)
```

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

You can also batch (i.e. Cassandra's definition of the word) mutations together by using `+`:
```scala
val batch: CQL[MutationResult]         = insert(p1) + update(p2.id, newPInfo) + insert(p3)
val unloggedBatch: CQL[MutationResult] = CQL.unlogged(batch)
```

Note: You cannot batch together queries and mutations as this is not allowed by Cassandra.

Now that we have built our CQL queries and mutations, we can execute them:
```scala
import zio._
import zio.stream._

// A single element stream is returned
val insertResult: ZStream[Has[CQLExecutor], Throwable, MutationResult] = insert(person).execute

// A stream of results is returned
val queryResult: ZStream[Has[CQLExecutor], Throwable, Person] = selectAll.execute
```

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

### Why the name Virgil?
Virgil was an ancient Roman poet who composed an epic poem about Cassandra and so we thought it would be appropriate.

### Inspiration
We were heavily inspired by Doobie, Cassandra4IO and Quill and wanted a more native ZIO solution for Cassandra focused
on ergonomics, ease of use and performance (compile-time and runtime). Special thanks to John De Goes, Francis Toth and
Nigel Benns for their help, mentorship, and guidance.
