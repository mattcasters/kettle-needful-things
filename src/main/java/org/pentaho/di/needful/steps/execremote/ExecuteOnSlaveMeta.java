package org.pentaho.di.needful.steps.execremote;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.needful.shared.ParameterDetails;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "ExecuteOnSlave",
  name = "Execute on slave server",
  description = "Execute a job or a transformation on a slave server",
  image = "ui/images/TRNEx.svg",
  categoryDescription = "Flow"
)
public class ExecuteOnSlaveMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String SLAVE_FIELD = "slave_field";
  public static final String FILENAME_FIELD = "filename_field";
  public static final String EXPORT_RESOURCES = "export_resources";
  public static final String DONT_WAIT = "do_not_wait";
  public static final String REPORT_REMOTE_LOG = "report_remote_log";
  public static final String RESULT_XML_FIELD = "result_xml_field";

  public static final String PARAMETERS = "parameters";
  public static final String PARAMETER = "parameter";


  private String slaveField;
  private String filenameField;
  private boolean exportingResources;
  private boolean notWaiting;
  private boolean reportingRemoteLog;
  private List<ParameterDetails> parameters;
  private String resultXmlField;

  public ExecuteOnSlaveMeta() {
    parameters = new ArrayList<>();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {
    if ( StringUtils.isNotEmpty( resultXmlField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaString( resultXmlField ) );
    }
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new ExecuteOnSlave( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new ExecuteOnSlaveData();
  }

  @Override public String getDialogClassName() {
    return ExecuteOnSlaveDialog.class.getName();
  }

  @Override public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.addTagValue( SLAVE_FIELD, slaveField ) );
    xml.append( XMLHandler.addTagValue( FILENAME_FIELD, filenameField ) );
    xml.append( XMLHandler.addTagValue( EXPORT_RESOURCES, exportingResources ) );
    xml.append( XMLHandler.addTagValue( DONT_WAIT, notWaiting) );
    xml.append( XMLHandler.addTagValue( REPORT_REMOTE_LOG, reportingRemoteLog) );
    xml.append( XMLHandler.openTag( PARAMETERS ) );
    for ( ParameterDetails parameter : parameters ) {
      xml.append( XMLHandler.openTag( PARAMETER ) );
      xml.append( XMLHandler.addTagValue( "name", parameter.getName() ) );
      xml.append( XMLHandler.addTagValue( "field", parameter.getField() ) );
      xml.append( XMLHandler.closeTag( PARAMETER ) );
    }
    xml.append( XMLHandler.closeTag( PARAMETERS ) );
    xml.append( XMLHandler.addTagValue( RESULT_XML_FIELD, resultXmlField ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    slaveField = XMLHandler.getTagValue( stepnode, SLAVE_FIELD );
    filenameField = XMLHandler.getTagValue( stepnode, FILENAME_FIELD );
    exportingResources = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, EXPORT_RESOURCES ) );
    notWaiting = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, DONT_WAIT ) );
    reportingRemoteLog = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, REPORT_REMOTE_LOG) );
    resultXmlField = XMLHandler.getTagValue( stepnode, RESULT_XML_FIELD );
    Node parametersNode = XMLHandler.getSubNode( stepnode, PARAMETERS );
    List<Node> parameterNodes = XMLHandler.getNodes( parametersNode, PARAMETER );
    parameters = new ArrayList<>();
    for ( Node parameterNode : parameterNodes ) {
      String name = XMLHandler.getTagValue( parameterNode, "name" );
      String field = XMLHandler.getTagValue( parameterNode, "field" );
      parameters.add( new ParameterDetails( name, field ) );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, SLAVE_FIELD, slaveField );
    rep.saveStepAttribute( id_transformation, id_step, FILENAME_FIELD, filenameField );
    rep.saveStepAttribute( id_transformation, id_step, EXPORT_RESOURCES, exportingResources );
    rep.saveStepAttribute( id_transformation, id_step, DONT_WAIT, notWaiting );
    rep.saveStepAttribute( id_transformation, id_step, REPORT_REMOTE_LOG, reportingRemoteLog );
    rep.saveStepAttribute( id_transformation, id_step, RESULT_XML_FIELD, resultXmlField );
    for ( int i = 0; i < parameters.size(); i++ ) {
      ParameterDetails parameter = parameters.get( i );
      rep.saveStepAttribute( id_transformation, id_step, i, PARAMETER + "_name", parameters.get( i ).getName() );
      rep.saveStepAttribute( id_transformation, id_step, i, PARAMETER + "_field", parameters.get( i ).getField() );
    }
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    slaveField = rep.getStepAttributeString( id_step, SLAVE_FIELD );
    filenameField = rep.getStepAttributeString( id_step, FILENAME_FIELD );
    exportingResources = rep.getStepAttributeBoolean( id_step, EXPORT_RESOURCES );
    notWaiting = rep.getStepAttributeBoolean( id_step, DONT_WAIT );
    reportingRemoteLog = rep.getStepAttributeBoolean( id_step, REPORT_REMOTE_LOG );
    resultXmlField = rep.getStepAttributeString( id_step, RESULT_XML_FIELD );
    parameters = new ArrayList<>();
    int nr = rep.countNrStepAttributes( id_step, PARAMETER + "_name" );
    for ( int i = 0; i < nr; i++ ) {
      String name = rep.getStepAttributeString( id_step, i, PARAMETER + "_name" );
      String field = rep.getStepAttributeString( id_step, i, PARAMETER + "_field" );
      parameters.add( new ParameterDetails( name, field ) );
    }
  }

  @Override public void setDefault() {
    slaveField = "";
    filenameField = "";
    exportingResources = false;
    resultXmlField = "resultXml";
    parameters = new ArrayList<>();
    notWaiting = false;
    reportingRemoteLog = true;
  }

  /**
   * Gets slaveField
   *
   * @return value of slaveField
   */
  public String getSlaveField() {
    return slaveField;
  }

  /**
   * @param slaveField The slaveField to set
   */
  public void setSlaveField( String slaveField ) {
    this.slaveField = slaveField;
  }

  /**
   * Gets filenameField
   *
   * @return value of filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /**
   * @param filenameField The filenameField to set
   */
  public void setFilenameField( String filenameField ) {
    this.filenameField = filenameField;
  }

  /**
   * Gets exportingResources
   *
   * @return value of exportingResources
   */
  public boolean isExportingResources() {
    return exportingResources;
  }

  /**
   * @param exportingResources The exportingResources to set
   */
  public void setExportingResources( boolean exportingResources ) {
    this.exportingResources = exportingResources;
  }

  /**
   * Gets parameters
   *
   * @return value of parameters
   */
  public List<ParameterDetails> getParameters() {
    return parameters;
  }

  /**
   * @param parameters The parameters to set
   */
  public void setParameters( List<ParameterDetails> parameters ) {
    this.parameters = parameters;
  }

  /**
   * Gets resultXmlField
   *
   * @return value of resultXmlField
   */
  public String getResultXmlField() {
    return resultXmlField;
  }

  /**
   * @param resultXmlField The resultXmlField to set
   */
  public void setResultXmlField( String resultXmlField ) {
    this.resultXmlField = resultXmlField;
  }

  /**
   * Gets notWaiting
   *
   * @return value of notWaiting
   */
  public boolean isNotWaiting() {
    return notWaiting;
  }

  /**
   * @param notWaiting The notWaiting to set
   */
  public void setNotWaiting( boolean notWaiting ) {
    this.notWaiting = notWaiting;
  }

  /**
   * Gets reportingRemoteLog
   *
   * @return value of reportingRemoteLog
   */
  public boolean isReportingRemoteLog() {
    return reportingRemoteLog;
  }

  /**
   * @param reportingRemoteLog The reportingRemoteLog to set
   */
  public void setReportingRemoteLog( boolean reportingRemoteLog ) {
    this.reportingRemoteLog = reportingRemoteLog;
  }
}
