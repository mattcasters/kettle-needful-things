package org.pentaho.di.needful.shared;

public class ParameterDetails {
  private String name;
  private String field;

  public ParameterDetails() {
  }

  public ParameterDetails( String name, String field ) {
    this.name = name;
    this.field = field;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets field
   *
   * @return value of field
   */
  public String getField() {
    return field;
  }

  /**
   * @param field The field to set
   */
  public void setField( String field ) {
    this.field = field;
  }
}
