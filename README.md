# beam-sdks-java-io-astra

Apache Beam SDK to work with Astra Pipelines

## How To

### Installation

To use this SDK, add the following dependency to your project:

![GitHub release (with filter)](https://img.shields.io/github/v/release/datastax-examples/beam-sdks-java-io-astra?label=latest%20release&color=green&link=https%3A%2F%2Fgithub.com%2FDataStax-Examples%2Fbeam-sdks-java-io-astra%2Freleases)


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

---
