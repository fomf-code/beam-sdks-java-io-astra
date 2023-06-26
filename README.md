# beam-sdks-java-io-astra

Apache Beam SDK to work with Astra Pipelines

## How To

### Installation

To use this SDK, add the following dependency to your project:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.datastax.astra/com.datastax.astra/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.datastax.astra/beam-sdks-java-io-astra)

```xml
<dependency>
  <groupId>com.datastax.astra</groupId>
  <artifactId>beam-sdks-java-io-astra</artifactId>
  <version>${latest-version}</version>
</dependency>
```

### Usage

Documentation is avalailable in [Awesome Astra](https://awesome-astra.github.io/docs/pages/tools/integration/apache-beam-google-dataflow/) with sample codes


- **Read Data From Astra**

```java
// LanguageCode is a sample Pojo

// LanguageCodeDaoMapperFactoryFn implements 
// SerializableFunction<CqlSession, AstraDbMapper<LanguageCode>>

// Get binary from File path
byte[] scbZip = AstraSecureConnectBundleUtils
                .loadFromFilePath(options.getAstraSecureConnectBundle());

AstraDbIO.Read<LanguageCode> read = AstraDbIO.<LanguageCode>read()
  .withToken(options.getAstraToken())
  .withKeyspace(options.getAstraKeyspace())
  .withSecureConnectBundle(scbZip)
  .withTable(options.getTable())
  .withCoder(SerializableCoder.of(LanguageCode.class))
  .withMapperFactoryFn(new LanguageCodeDaoMapperFactoryFn())
  .withEntity(LanguageCode.class))
```






