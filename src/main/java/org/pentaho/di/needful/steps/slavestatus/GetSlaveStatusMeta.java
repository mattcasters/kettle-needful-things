package org.pentaho.di.needful.steps.slavestatus;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.www.GetStatusServlet;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@Step(
  id = "GetSlaveStatus",
  name = "Get slave server status",
  description = "Get the status from a chosen slave server",
  image = "ui/images/WSA.svg",
  categoryDescription = "Lookup"
)
public class GetSlaveStatusMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String SLAVE_FIELD = "slave_field";
  public static final String ERROR_MESSAGE_FIELD = "error_message_field";
  public static final String STATUS_DESCRIPTION_FIELD = "status_description_field";
  public static final String SERVER_LOAD_FIELD = "server_load_field";
  public static final String MEMORY_FREE_FIELD = "memory_free_field";
  public static final String MEMORY_TOTAL_FIELD = "memory_total_field";
  public static final String CPU_CORES_FIELD = "cpu_cores_field";
  public static final String CPU_PROCESS_TIME_FIELD = "cpu_process_time_field";
  public static final String OS_NAME_FIELD = "os_name_field";
  public static final String OS_VERSION_FIELD = "os_version_field";
  public static final String OS_ARCHITECTURE_FIELD = "os_architecture_field";
  public static final String ACTIVE_TRANSFORMATIONS_FIELD = "active_transformations_field";
  public static final String ACTIVE_JOBS_FIELD = "active_jobs_field";
  public static final String AVAILABLE_FIELD = "available_field";
  public static final String RESPONSE_NS_FIELD = "response_ns_field";

  private String slaveField;
  private String errorMessageField;
  private String statusDescriptionField;
  private String serverLoadField;
  private String memoryFreeField;
  private String memoryTotalField;
  private String cpuCoresField;
  private String cpuProcessTimeField;
  private String osNameField;
  private String osVersionField;
  private String osArchitectureField;
  private String activeTransformationsField;
  private String activeJobsField;
  private String availableField;
  private String responseNsField;


  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {
    if ( StringUtils.isNotEmpty( errorMessageField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaString( errorMessageField ) );
    }
    if ( StringUtils.isNotEmpty( statusDescriptionField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaString( statusDescriptionField ) );
    }
    if ( StringUtils.isNotEmpty( serverLoadField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaNumber( serverLoadField ) );
    }
    if ( StringUtils.isNotEmpty( memoryFreeField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaInteger( memoryFreeField ) );
    }
    if ( StringUtils.isNotEmpty( memoryTotalField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaInteger( memoryTotalField ) );
    }
    if ( StringUtils.isNotEmpty( cpuCoresField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaInteger( cpuCoresField ) );
    }
    if ( StringUtils.isNotEmpty( cpuProcessTimeField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaInteger( cpuProcessTimeField ) );
    }
    if ( StringUtils.isNotEmpty( osNameField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaString( osNameField ) );
    }
    if ( StringUtils.isNotEmpty( osVersionField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaString( osVersionField ) );
    }
    if ( StringUtils.isNotEmpty( osArchitectureField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaString( osArchitectureField ) );
    }
    if ( StringUtils.isNotEmpty( activeTransformationsField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaInteger( activeTransformationsField ) );
    }
    if ( StringUtils.isNotEmpty( activeJobsField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaInteger( activeJobsField ) );
    }
    if ( StringUtils.isNotEmpty( availableField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaBoolean( availableField ) );
    }
    if ( StringUtils.isNotEmpty( responseNsField ) ) {
      inputRowMeta.addValueMeta( new ValueMetaInteger( responseNsField ) );
    }
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new GetSlaveStatus( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new GetSlaveStatusData();
  }

  @Override public String getDialogClassName() {
    return GetSlaveStatusDialog.class.getName();
  }

  @Override public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.addTagValue( SLAVE_FIELD, slaveField ) );
    xml.append( XMLHandler.addTagValue( ERROR_MESSAGE_FIELD, errorMessageField ) );
    xml.append( XMLHandler.addTagValue( STATUS_DESCRIPTION_FIELD, statusDescriptionField ) );
    xml.append( XMLHandler.addTagValue( SERVER_LOAD_FIELD, serverLoadField ) );
    xml.append( XMLHandler.addTagValue( MEMORY_FREE_FIELD, memoryFreeField ) );
    xml.append( XMLHandler.addTagValue( MEMORY_TOTAL_FIELD, memoryTotalField ) );
    xml.append( XMLHandler.addTagValue( CPU_CORES_FIELD, cpuCoresField ) );
    xml.append( XMLHandler.addTagValue( CPU_PROCESS_TIME_FIELD, cpuProcessTimeField ) );
    xml.append( XMLHandler.addTagValue( OS_NAME_FIELD, osNameField ) );
    xml.append( XMLHandler.addTagValue( OS_VERSION_FIELD, osVersionField ) );
    xml.append( XMLHandler.addTagValue( OS_ARCHITECTURE_FIELD, osArchitectureField ) );
    xml.append( XMLHandler.addTagValue( ACTIVE_TRANSFORMATIONS_FIELD, activeTransformationsField ) );
    xml.append( XMLHandler.addTagValue( ACTIVE_JOBS_FIELD, activeJobsField ) );
    xml.append( XMLHandler.addTagValue( AVAILABLE_FIELD, availableField ) );
    xml.append( XMLHandler.addTagValue( RESPONSE_NS_FIELD, responseNsField ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    slaveField = XMLHandler.getTagValue( stepnode, SLAVE_FIELD );
    errorMessageField = XMLHandler.getTagValue( stepnode, ERROR_MESSAGE_FIELD );
    statusDescriptionField = XMLHandler.getTagValue( stepnode, STATUS_DESCRIPTION_FIELD );
    serverLoadField = XMLHandler.getTagValue( stepnode, SERVER_LOAD_FIELD );
    memoryFreeField = XMLHandler.getTagValue( stepnode, MEMORY_FREE_FIELD );
    memoryTotalField = XMLHandler.getTagValue( stepnode, MEMORY_TOTAL_FIELD );
    cpuCoresField = XMLHandler.getTagValue( stepnode, CPU_CORES_FIELD );
    cpuProcessTimeField = XMLHandler.getTagValue( stepnode, CPU_PROCESS_TIME_FIELD );
    osNameField = XMLHandler.getTagValue( stepnode, OS_NAME_FIELD );
    osVersionField = XMLHandler.getTagValue( stepnode, OS_VERSION_FIELD );
    osArchitectureField = XMLHandler.getTagValue( stepnode, OS_ARCHITECTURE_FIELD );
    activeTransformationsField = XMLHandler.getTagValue( stepnode, ACTIVE_TRANSFORMATIONS_FIELD );
    activeJobsField = XMLHandler.getTagValue( stepnode, ACTIVE_JOBS_FIELD );
    availableField = XMLHandler.getTagValue( stepnode, AVAILABLE_FIELD );
    responseNsField = XMLHandler.getTagValue( stepnode, RESPONSE_NS_FIELD );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, SLAVE_FIELD, slaveField );
    rep.saveStepAttribute( id_transformation, id_step, ERROR_MESSAGE_FIELD, errorMessageField );
    rep.saveStepAttribute( id_transformation, id_step, STATUS_DESCRIPTION_FIELD, statusDescriptionField );
    rep.saveStepAttribute( id_transformation, id_step, SERVER_LOAD_FIELD, serverLoadField );
    rep.saveStepAttribute( id_transformation, id_step, MEMORY_FREE_FIELD, memoryFreeField );
    rep.saveStepAttribute( id_transformation, id_step, MEMORY_TOTAL_FIELD, memoryTotalField );
    rep.saveStepAttribute( id_transformation, id_step, CPU_CORES_FIELD, cpuCoresField );
    rep.saveStepAttribute( id_transformation, id_step, CPU_PROCESS_TIME_FIELD, cpuProcessTimeField );
    rep.saveStepAttribute( id_transformation, id_step, OS_NAME_FIELD, osNameField );
    rep.saveStepAttribute( id_transformation, id_step, OS_VERSION_FIELD, osVersionField );
    rep.saveStepAttribute( id_transformation, id_step, OS_ARCHITECTURE_FIELD, osArchitectureField );
    rep.saveStepAttribute( id_transformation, id_step, ACTIVE_TRANSFORMATIONS_FIELD, activeTransformationsField );
    rep.saveStepAttribute( id_transformation, id_step, ACTIVE_JOBS_FIELD, activeJobsField );
    rep.saveStepAttribute( id_transformation, id_step, AVAILABLE_FIELD, availableField );
    rep.saveStepAttribute( id_transformation, id_step, RESPONSE_NS_FIELD, responseNsField );
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    slaveField = rep.getStepAttributeString( id_step, SLAVE_FIELD );
    errorMessageField = rep.getStepAttributeString( id_step, ERROR_MESSAGE_FIELD );
    statusDescriptionField = rep.getStepAttributeString( id_step, STATUS_DESCRIPTION_FIELD );
    serverLoadField = rep.getStepAttributeString( id_step, SERVER_LOAD_FIELD );
    memoryFreeField = rep.getStepAttributeString( id_step, MEMORY_FREE_FIELD );
    memoryTotalField = rep.getStepAttributeString( id_step, MEMORY_TOTAL_FIELD );
    cpuCoresField = rep.getStepAttributeString( id_step, CPU_CORES_FIELD );
    cpuProcessTimeField = rep.getStepAttributeString( id_step, CPU_PROCESS_TIME_FIELD );
    osNameField = rep.getStepAttributeString( id_step, OS_NAME_FIELD );
    osVersionField = rep.getStepAttributeString( id_step, OS_VERSION_FIELD );
    osArchitectureField = rep.getStepAttributeString( id_step, OS_ARCHITECTURE_FIELD );
    activeTransformationsField = rep.getStepAttributeString( id_step, ACTIVE_TRANSFORMATIONS_FIELD );
    activeJobsField = rep.getStepAttributeString( id_step, ACTIVE_JOBS_FIELD );
    availableField = rep.getStepAttributeString( id_step, AVAILABLE_FIELD );
    responseNsField = rep.getStepAttributeString( id_step, RESPONSE_NS_FIELD );
  }

  @Override public void setDefault() {
    slaveField = "";
    errorMessageField = "errorMessage";
    statusDescriptionField = "statusDescription";
    serverLoadField = "serverLoad";
    memoryFreeField = "memoryFree";
    memoryTotalField = "memoryTotal";
    cpuCoresField = "cpuCores";
    cpuProcessTimeField = "cpuProcessTime";
    osNameField = "osName";
    osVersionField = "osVersion";
    osArchitectureField = "osArchitecture";
    activeTransformationsField = "activeTransformations";
    activeJobsField = "activeJobs";
    availableField = "available";
    responseNsField = "responseNs";
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
   * Gets errorMessageField
   *
   * @return value of errorMessageField
   */
  public String getErrorMessageField() {
    return errorMessageField;
  }

  /**
   * @param errorMessageField The errorMessageField to set
   */
  public void setErrorMessageField( String errorMessageField ) {
    this.errorMessageField = errorMessageField;
  }

  /**
   * Gets statusDescriptionField
   *
   * @return value of statusDescriptionField
   */
  public String getStatusDescriptionField() {
    return statusDescriptionField;
  }

  /**
   * @param statusDescriptionField The statusDescriptionField to set
   */
  public void setStatusDescriptionField( String statusDescriptionField ) {
    this.statusDescriptionField = statusDescriptionField;
  }

  /**
   * Gets serverLoadField
   *
   * @return value of serverLoadField
   */
  public String getServerLoadField() {
    return serverLoadField;
  }

  /**
   * @param serverLoadField The serverLoadField to set
   */
  public void setServerLoadField( String serverLoadField ) {
    this.serverLoadField = serverLoadField;
  }

  /**
   * Gets memoryFreeField
   *
   * @return value of memoryFreeField
   */
  public String getMemoryFreeField() {
    return memoryFreeField;
  }

  /**
   * @param memoryFreeField The memoryFreeField to set
   */
  public void setMemoryFreeField( String memoryFreeField ) {
    this.memoryFreeField = memoryFreeField;
  }

  /**
   * Gets memoryTotalField
   *
   * @return value of memoryTotalField
   */
  public String getMemoryTotalField() {
    return memoryTotalField;
  }

  /**
   * @param memoryTotalField The memoryTotalField to set
   */
  public void setMemoryTotalField( String memoryTotalField ) {
    this.memoryTotalField = memoryTotalField;
  }

  /**
   * Gets cpuCoresField
   *
   * @return value of cpuCoresField
   */
  public String getCpuCoresField() {
    return cpuCoresField;
  }

  /**
   * @param cpuCoresField The cpuCoresField to set
   */
  public void setCpuCoresField( String cpuCoresField ) {
    this.cpuCoresField = cpuCoresField;
  }

  /**
   * Gets cpuProcessTimeField
   *
   * @return value of cpuProcessTimeField
   */
  public String getCpuProcessTimeField() {
    return cpuProcessTimeField;
  }

  /**
   * @param cpuProcessTimeField The cpuProcessTimeField to set
   */
  public void setCpuProcessTimeField( String cpuProcessTimeField ) {
    this.cpuProcessTimeField = cpuProcessTimeField;
  }

  /**
   * Gets osNameField
   *
   * @return value of osNameField
   */
  public String getOsNameField() {
    return osNameField;
  }

  /**
   * @param osNameField The osNameField to set
   */
  public void setOsNameField( String osNameField ) {
    this.osNameField = osNameField;
  }

  /**
   * Gets osVersionField
   *
   * @return value of osVersionField
   */
  public String getOsVersionField() {
    return osVersionField;
  }

  /**
   * @param osVersionField The osVersionField to set
   */
  public void setOsVersionField( String osVersionField ) {
    this.osVersionField = osVersionField;
  }

  /**
   * Gets osArchitectureField
   *
   * @return value of osArchitectureField
   */
  public String getOsArchitectureField() {
    return osArchitectureField;
  }

  /**
   * @param osArchitectureField The osArchitectureField to set
   */
  public void setOsArchitectureField( String osArchitectureField ) {
    this.osArchitectureField = osArchitectureField;
  }

  /**
   * Gets activeTransformationsField
   *
   * @return value of activeTransformationsField
   */
  public String getActiveTransformationsField() {
    return activeTransformationsField;
  }

  /**
   * @param activeTransformationsField The activeTransformationsField to set
   */
  public void setActiveTransformationsField( String activeTransformationsField ) {
    this.activeTransformationsField = activeTransformationsField;
  }

  /**
   * Gets activeJobsField
   *
   * @return value of activeJobsField
   */
  public String getActiveJobsField() {
    return activeJobsField;
  }

  /**
   * @param activeJobsField The activeJobsField to set
   */
  public void setActiveJobsField( String activeJobsField ) {
    this.activeJobsField = activeJobsField;
  }

  /**
   * Gets availableField
   *
   * @return value of availableField
   */
  public String getAvailableField() {
    return availableField;
  }

  /**
   * @param availableField The availableField to set
   */
  public void setAvailableField( String availableField ) {
    this.availableField = availableField;
  }

  /**
   * Gets responseNsField
   *
   * @return value of responseNsField
   */
  public String getResponseNsField() {
    return responseNsField;
  }

  /**
   * @param responseNsField The responseNsField to set
   */
  public void setResponseNsField( String responseNsField ) {
    this.responseNsField = responseNsField;
  }
}
