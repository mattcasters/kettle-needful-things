package org.pentaho.di.needful.steps.multistreamlookup;

import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.Objects;

public class LookupResult {

  private String datasetField;

  private String renameTo;

  public LookupResult() {
  }

  public LookupResult( String datasetField, String renameTo ) {
    this.datasetField = datasetField;
    this.renameTo = renameTo;
  }

  public LookupResult( LookupResult r ) {
    this.datasetField = r.datasetField;
    this.renameTo = r.renameTo;
  }

  @Override public String toString() {
    return "LookupResult{" +
      "datasetField='" + datasetField + '\'' +
      ", renameTo='" + renameTo + '\'' +
      '}';
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    LookupResult that = (LookupResult) o;
    return Objects.equals( datasetField, that.datasetField ) &&
      Objects.equals( renameTo, that.renameTo );
  }

  @Override public int hashCode() {
    return Objects.hash( datasetField, renameTo );
  }

  /**
   * Gets datasetField
   *
   * @return value of datasetField
   */
  public String getDatasetField() {
    return datasetField;
  }

  /**
   * @param datasetField The datasetField to set
   */
  public void setDatasetField( String datasetField ) {
    this.datasetField = datasetField;
  }

  /**
   * Gets renameTo
   *
   * @return value of renameTo
   */
  public String getRenameTo() {
    return renameTo;
  }

  /**
   * @param renameTo The renameTo to set
   */
  public void setRenameTo( String renameTo ) {
    this.renameTo = renameTo;
  }
}
