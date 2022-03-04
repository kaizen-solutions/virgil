package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.annotations.{CqlColumn, CqlDiscriminator}
import io.kaizensolutions.virgil.cql.CqlStringContext
import io.kaizensolutions.virgil.dsl.{SelectBuilder, _}
import zio.Has
import zio.random.Random
import zio.schema.{DeriveSchema, Schema}
import zio.stream.ZStream
import zio.test.Assertion.hasSameElements
import zio.test.TestAspect.{samples, sequential}
import zio.test._

object SumTypeSpec {
  def sumTypeSpec: ZSpec[Has[CQLExecutor] with Random with Sized with TestConfig, Any] =
    (nonDiscriminator + discriminator) @@ samples(10)

  def discriminator: ZSpec[Has[CQLExecutor] with Random with Sized with TestConfig, Any] =
    suite("Sum Type Specification") {
      suite("Discriminator aided sum types") {
        testM("be able to persist and retrieve sum types encoded within product types") {
          checkM(Gen.chunkOfN(5)(ConfigurationData.gen)) { configs =>
            for {
              _     <- ConfigurationData.truncate.execute.runDrain
              empty <- ConfigurationData.selectAll.execute.runCollect
              _     <- ZStream.fromIterable(configs).flatMapPar(16)(ConfigurationData.insert(_).execute).runDrain
              all   <- ConfigurationData.selectAll.execute.runCollect
              allByIds <-
                ZStream.fromIterable(configs).flatMap(c => ConfigurationData.select(c.id).execute).runCollect
            } yield assertTrue(empty.isEmpty) &&
              assert(all)(Assertion.hasSameElements(configs)) &&
              assert(allByIds)(Assertion.hasSameElements(configs))
          }
        }
      }
    }

  def nonDiscriminator: ZSpec[Has[CQLExecutor] with Random with Sized with TestConfig, Any] =
    suite("First matching sum type") {
      testM("be able to persist and retrieve sum types encoded within product types") {
        checkM(Gen.chunkOfN(3)(ConfigurationDataNonDiscriminator.genAll)) { configs =>
          val truncate =
            ConfigurationDataNonDiscriminator.truncate.execute.runDrain

          val inserts = ZStream
            .fromChunk(configs)
            .flatMapPar(16)(config => ConfigurationDataNonDiscriminator.insert(config).execute)
            .runDrain

          val selectAll =
            ConfigurationDataNonDiscriminator.selectAll.execute.runCollect

          val selectByIds =
            ZStream
              .fromChunk(configs)
              .map(_.id)
              .flatMap(ConfigurationDataNonDiscriminator.select(_).execute)
              .runCollect

          for {
            _     <- truncate
            _     <- inserts
            all   <- selectAll
            byIds <- selectByIds
          } yield assert(all)(hasSameElements(configs)) && assert(byIds)(hasSameElements(configs))
        }
      } +
        testM("provide detailed errors if all decoders do not match") {
          val truncate = ConfigurationDataNonDiscriminator.truncate.execute.runDrain
          val invalid =
            cql"INSERT INTO sumtypespec_configuration_data_no_discriminator (id) VALUES (1)".mutation.execute.runDrain
          val select = ConfigurationDataNonDiscriminator.select(1).execute.runHead

          for {
            _      <- truncate
            _      <- invalid
            error  <- select.flip
            message = error.getMessage
          } yield assertTrue(
            message.contains(
              """Tried the following decoders: Cannot invoke "com.datastax.oss.driver.api.core.data.GettableByName.getString(String)" because "structure" is null and then tried Cannot invoke "com.datastax.oss.driver.api.core.data.GettableByName.getString(String)" because "structure" is null and then tried Cannot invoke "com.datastax.oss.driver.api.core.data.GettableByName.getString(String)" because "structure" is null""".stripMargin
            )
          )
        }
    } @@ sequential
}

final case class ConfigurationData(id: Int, config: Configuration)
object ConfigurationData {
  implicit val schemaConfigurationData: Schema[ConfigurationData] = DeriveSchema.gen[ConfigurationData]

  private val tableName = "sumtypespec_configuration_data"
  def selectAll: CQL[ConfigurationData] =
    SelectBuilder
      .from(tableName)
      .columns("id", "config")
      .build[ConfigurationData]

  def select(id: Int): CQL[ConfigurationData] =
    SelectBuilder
      .from(tableName)
      .columns("id", "config")
      .where("id" === id)
      .build[ConfigurationData]

  def insert(in: ConfigurationData): CQL[MutationResult] =
    InsertBuilder(tableName)
      .value("id", in.id)
      .value("config", in.config)
      .build

  def truncate: CQL[MutationResult] =
    CQL.truncate(tableName)

  def gen: Gen[Random with Sized, ConfigurationData] =
    for {
      id     <- Gen.int(1, 1000000)
      config <- Gen.oneOf(Configuration.FileConfig.gen, Configuration.CloudConfig.gen)
    } yield ConfigurationData(id, config)
}

@CqlDiscriminator("config_type")
sealed trait Configuration
object Configuration {
  implicit val schemaConfiguration: Schema[Configuration] =
    DeriveSchema.gen[Configuration]

  final case class CloudConfig(@CqlColumn("api_id") id: String, @CqlColumn("api_key") secret: String)
      extends Configuration
  object CloudConfig {
    implicit val schemaCloudConfig: Schema[CloudConfig] = DeriveSchema.gen[CloudConfig]

    def gen: Gen[Random, CloudConfig] = for {
      id     <- Gen.stringBounded(2, 8)(Gen.alphaChar)
      secret <- Gen.stringBounded(8, 16)(Gen.alphaNumericChar)
    } yield CloudConfig(id, secret)
  }

  final case class FileConfig(@CqlColumn("file_path") path: String, metadata: Map[String, Set[Metadata]])
      extends Configuration
  object FileConfig {
    implicit val schemaFileConfig: Schema[FileConfig] =
      DeriveSchema.gen[FileConfig]

    def gen: Gen[Random with Sized, FileConfig] = for {
      path <- Gen.stringBounded(2, 8)(Gen.alphaChar)
      meta <- Gen.mapOf(Gen.stringBounded(2, 8)(Gen.alphaChar), Gen.setOf(Metadata.gen))
    } yield FileConfig(path, meta)
  }
}

final case class ConfigurationDataNonDiscriminator(id: Int, config: ConfigurationNoDiscriminator)
object ConfigurationDataNonDiscriminator {
  implicit val schemaConfigurationNoDiscriminator: Schema[ConfigurationDataNonDiscriminator] =
    DeriveSchema.gen[ConfigurationDataNonDiscriminator]

  private val tableName = "sumtypespec_configuration_data_no_discriminator"
  def selectAll: CQL[ConfigurationDataNonDiscriminator] =
    SelectBuilder
      .from(tableName)
      .columns("id", "config")
      .build[ConfigurationDataNonDiscriminator]

  def select(id: Int): CQL[ConfigurationDataNonDiscriminator] =
    SelectBuilder
      .from(tableName)
      .columns("id", "config")
      .where("id" === id)
      .build[ConfigurationDataNonDiscriminator]

  def insert(in: ConfigurationDataNonDiscriminator): CQL[MutationResult] =
    InsertBuilder(tableName)
      .value("id", in.id)
      .value("config", in.config)
      .build

  def truncate: CQL[MutationResult] =
    CQL.truncate(tableName)

  def genAll: Gen[Random with Sized, ConfigurationDataNonDiscriminator] =
    genSpecificConfig(ConfigurationNoDiscriminator.genAll)

  def genSpecificConfig(
    in: Gen[Random with Sized, ConfigurationNoDiscriminator]
  ): Gen[Random with Sized, ConfigurationDataNonDiscriminator] =
    for {
      id     <- Gen.int(1, 1000000)
      config <- in
    } yield ConfigurationDataNonDiscriminator(id, config)
}

sealed trait ConfigurationNoDiscriminator
object ConfigurationNoDiscriminator {
  implicit val schemaConfigurationNoDiscriminator: Schema[ConfigurationNoDiscriminator] =
    DeriveSchema.gen[ConfigurationNoDiscriminator]

  def genAll: Gen[Random with Sized, ConfigurationNoDiscriminator] =
    Gen.oneOf(NetworkConfig.gen, FileConfig.gen, DefaultConfig.gen)

  final case class NetworkConfig(@CqlColumn("network_id") id: String, @CqlColumn("network_key") secret: String)
      extends ConfigurationNoDiscriminator
  object NetworkConfig {
    implicit val schemaNetworkConfig: Schema[NetworkConfig] = DeriveSchema.gen[NetworkConfig]

    def gen: Gen[Random, NetworkConfig] = for {
      id     <- Gen.stringBounded(2, 8)(Gen.alphaChar)
      secret <- Gen.stringBounded(8, 16)(Gen.alphaNumericChar)
    } yield NetworkConfig(id, secret)
  }

  final case class FileConfig(@CqlColumn("file_path") path: String, metadata: Map[String, Set[Metadata]])
      extends ConfigurationNoDiscriminator
  object FileConfig {
    implicit val schemaFileConfig: Schema[FileConfig] =
      DeriveSchema.gen[FileConfig]

    def gen: Gen[Random with Sized, FileConfig] = for {
      path <- Gen.stringBounded(2, 8)(Gen.alphaChar)
      meta <- Gen.mapOf(Gen.stringBounded(2, 8)(Gen.alphaChar), Gen.setOf(Metadata.gen))
    } yield FileConfig(path, meta)
  }

  final case class DefaultConfig(
    @CqlColumn("default_id") defaultId: String,
    @CqlColumn("default_password") defaultPassword: String
  ) extends ConfigurationNoDiscriminator
  object DefaultConfig {
    implicit val schemaDefaultConfig: Schema[DefaultConfig] = DeriveSchema.gen[DefaultConfig]

    def gen: Gen[Random, DefaultConfig] = for {
      id     <- Gen.stringBounded(2, 8)(Gen.alphaChar)
      secret <- Gen.stringBounded(8, 16)(Gen.alphaNumericChar)
    } yield DefaultConfig(id, secret)
  }
}

final case class Metadata(key: String, value: String)
object Metadata {
  implicit val schemaMetadata: Schema[Metadata] = DeriveSchema.gen[Metadata]

  def gen: Gen[Random, Metadata] = for {
    key   <- Gen.stringBounded(2, 8)(Gen.alphaChar)
    value <- Gen.stringBounded(2, 8)(Gen.alphaChar)
  } yield Metadata(key, value)
}
