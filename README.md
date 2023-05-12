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

To Write a PCollection to Astra, use the following code:

- You need to define a Bean in respect of the `cassandra-driver-mapping` rules. Those are described in the [referential documentation](https://docs.datastax.com/en/developer/java-driver/3.11/manual/object_mapper/creating/). As an example we can leverage on the following table:
```sql
CREATE TABLE IF NOT EXISTS simpledata (
    id int,
    data text,  
    PRIMARY KEY (id)
);  
```

- The associated bean will be the class [`SimpleDataEntity`](#) 

```java
@Table(name = "simpledata")
public class SimpleDataEntity implements Serializable {
    
    @PartitionKey
    protected int id;
    @Column
    protected String data;
    
    //Constructor, Getters and setters
}
```

- Create a Pipeline to read data From Astra
```java
```

- Create a Pipeline to write data into Astra
```java







