# Codecs (Encoders and Decoders)

Virgil uses a number of different encoders and decoders to serialize and deserialize data to/from Cassandra.

At the lowest level, we map the core data-types [supported by Cassandra](https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/#cql-to-java-type-mapping)
(including `UdtValue` and collections like List/Chunk/Vector & Map and & Set) and this is done in `CqlPrimitiveDecoder` 
and `CqlPrimitiveEncoder`.

For UDT Values, we have `CqlUdtValueDecoder` and `CqlUdtValueEncoder` which rely on `CqlPrimitiveDecoder` and 
`CqlPrimitiveEncoder` to decode and encode the User Defined Type values.

For Rows, we have `CqlRowDecoder` and `CqlRowEncoder` which also rely on `CqlPrimitiveDecoder` and 
`CqlPrimitiveEncoder` to decode and encode the rows. 

Rows and UDT Values can contain nested UDT Values and for this reason `CqlPrimitiveDecoder` and `CqlPrimitiveEncoder` 
can materialize instances of UDT Values. As a result, codec derivation takes place automatically provided you create
case classes (even nested ones) consisting entirely of the core types supported by Cassandra. If you want to map 
additional primitive types, you can implement `CqlPrimitiveDecoder` and `CqlPrimitiveEncoder` instances for your type.

If your case classes do not match the database schema 1:1, you can utilize `CqlUdtValueDecoder.cursor`/`CqlRowDecoder.cursor` 
or the lower level `CqlUdtValueDecoder.custom`/`CqlRowDecoder.custom` to map your data-types yourself.