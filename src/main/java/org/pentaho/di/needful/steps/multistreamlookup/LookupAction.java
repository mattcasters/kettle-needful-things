package org.pentaho.di.needful.steps.multistreamlookup;

import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Details how a lookup action is to be performed
 */
public class LookupAction {

  private String inputField;

  private String dataSetField;

  private List<LookupResult> lookupResults;

  public LookupAction() {
    lookupResults = new ArrayList<>(  );
  }

  public LookupAction( String inputField, String dataSetField, List<LookupResult> lookupResults ) {
    this.inputField = inputField;
    this.dataSetField = dataSetField;
    this.lookupResults = lookupResults;
  }

  @Override public String toString() {
    return "LookupAction{" +
      "inputField='" + inputField + '\'' +
      ", dataSetField='" + dataSetField + '\'' +
      '}';
  }

  public LookupAction( LookupAction lookupAction ) {
    this();
    this.inputField = lookupAction.inputField;
    this.dataSetField = lookupAction.dataSetField;
    for (LookupResult lookupResult : lookupAction.lookupResults) {
      lookupResults.add(new LookupResult( lookupResult ));
    }
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    LookupAction that = (LookupAction) o;
    return Objects.equals( inputField, that.inputField ) &&
      Objects.equals( dataSetField, that.dataSetField );
  }

  @Override public int hashCode() {
    return Objects.hash( inputField, dataSetField );
  }

  /**
   * Gets inputField
   *
   * @return value of inputField
   */
  public String getInputField() {
    return inputField;
  }

  /**
   * @param inputField The inputField to set
   */
  public void setInputField( String inputField ) {
    this.inputField = inputField;
  }

  /**
   * Gets dataSetField
   *
   * @return value of dataSetField
   */
  public String getDataSetField() {
    return dataSetField;
  }

  /**
   * @param dataSetField The dataSetField to set
   */
  public void setDataSetField( String dataSetField ) {
    this.dataSetField = dataSetField;
  }

  /**
   * Gets lookupResults
   *
   * @return value of lookupResults
   */
  public List<LookupResult> getLookupResults() {
    return lookupResults;
  }

  /**
   * @param lookupResults The lookupResults to set
   */
  public void setLookupResults( List<LookupResult> lookupResults ) {
    this.lookupResults = lookupResults;
  }
}
