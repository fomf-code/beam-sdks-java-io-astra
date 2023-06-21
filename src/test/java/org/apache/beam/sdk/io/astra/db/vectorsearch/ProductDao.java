package org.apache.beam.sdk.io.astra.db.vectorsearch;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;

@Dao
public interface ProductDao extends AstraDbMapper<Product> {}
