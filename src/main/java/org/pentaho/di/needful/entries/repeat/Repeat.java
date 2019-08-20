package org.pentaho.di.needful.entries.repeat;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.DelegationListener;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.job.JobEntryJob;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.needful.shared.ParameterDetails;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JobEntry(
  id = "Repeat",
  name = "Repeat",
  description = "Repeat execution of a job or a transformation",
  categoryDescription = "General",
  image = "ui/images/JOBEx.svg"
)
public class Repeat extends JobEntryBase implements JobEntryInterface, Cloneable {

  public static final String REPEAT_END_LOOP = "_REPEAT_END_LOOP_";

  public static final String FILENAME = "filename";
  public static final String VARIABLE_NAME = "variable_name";
  public static final String VARIABLE_VALUE = "variable_value";
  public static final String DELAY = "delay";
  public static final String KEEP_VALUES = "keep_values";

  public static final String PARAMETERS = "parameters";
  public static final String PARAMETER = "parameter";

  private String filename;
  private List<ParameterDetails> parameters;
  private String variableName;
  private String variableValue;
  private String delay;
  private boolean keepingValues;

  private class ExecutionResult {
    public Result result;
    public VariableSpace space;
    public boolean flagSet;

    public ExecutionResult( Result result, VariableSpace space, boolean flagSet ) {
      this.result = result;
      this.space = space;
      this.flagSet = flagSet;
    }
  }

  public Repeat( String name, String description) {
    super( name, description );
    parameters = new ArrayList<>();
  }

  public Repeat() {
    this( "", "" );
  }

  @Override public Repeat clone() {
    return (Repeat) super.clone();
  }

  @Override public Result execute( Result prevResult, int nr ) throws KettleException {

    // So now we execute the transformation or job and continue until the variable has a certain value.
    //
    String realFilename = environmentSubstitute( filename );
    if ( StringUtils.isEmpty( realFilename ) ) {
      throw new KettleException( "Please specify a transformation or job to repeat" );
    }

    long delayInMs = -1;

    if (StringUtils.isNotEmpty( delay )) {
      String realDelay = environmentSubstitute( delay );
      delayInMs = Const.toLong( realDelay, -1);
      if (delayInMs<0) {
        throw new KettleException( "Unable to parse delay for string: "+realDelay );
      }
      delayInMs*=1000; // convert to ms
    }

    // Clear flag
    //
    getExtensionDataMap().remove( REPEAT_END_LOOP );

    // If the variable is set at the beginning of the loop, don't execute at all!
    //
    if ( isVariableValueSet( this ) ) {
      return prevResult;
    }

    ExecutionResult executionResult = null;

    setBusyIcon();

    boolean repeat = true;
    while ( repeat && !parentJob.isStopped() ) {
      executionResult = executeTransformationOrJob( realFilename, nr, executionResult );
      Result result = executionResult.result;
      if ( !result.getResult() || result.getNrErrors()>0 || result.isStopped()) {
        log.logError("The repeating work encountered and error or was stopped. This ends the loop.");

        // On an false result, stop the loop
        //
        prevResult.setResult( false );

        repeat = false;
      } else {
        // End repeat if the End Repeat job entry is executed
        //
        if (executionResult.flagSet) {
          repeat = false;
        } else {
          // Repeat as long as the variable is not set.
          //
          repeat = !isVariableValueSet( executionResult.space );
        }
      }

      if (repeat && delayInMs>0) {
        // See if we need to delay
        //
        long startTime = System.currentTimeMillis();
        while (!parentJob.isStopped() && (System.currentTimeMillis()-startTime<delayInMs) ) {
          try {
            Thread.sleep( 100 );
          } catch(Exception e) {
            // Ignore
          }
        }
      }
    }

    clearBusyIcon();

    // Add last execution results
    //
    if ( executionResult != null ) {
      prevResult.add( executionResult.result );
    }

    return prevResult;
  }

  private void setBusyIcon() {
    // Set the busy icon the the graph
    //
    JobEntryTrans jeTrans = new JobEntryTrans(getName());
    JobEntryCopy key = new JobEntryCopy( jeTrans);
    parentJob.getActiveJobEntryTransformations().put(key, jeTrans);
  }

  private void clearBusyIcon() {
    // Remove the busy icon in the graph
    //
    JobEntryTrans jeTrans = new JobEntryTrans(getName());
    JobEntryCopy key = new JobEntryCopy( jeTrans);
    parentJob.getActiveJobEntryTransformations().remove(key);
  }

  private ExecutionResult executeTransformationOrJob( String realFilename, int nr, ExecutionResult previousResult ) throws KettleException {
    if ( isTransformation( realFilename ) ) {
      return executeTransformation( realFilename, nr, previousResult );
    }
    if ( isJob( realFilename ) ) {
      return executeJob( realFilename, nr, previousResult );
    }
    throw new KettleException( "Don't know if this is a transformation or a job" );
  }

  private ExecutionResult executeTransformation( String realFilename, int nr, ExecutionResult previousResult ) throws KettleException {
    TransMeta transMeta = loadTransformation( realFilename, rep, metaStore, this );
    Trans trans = new Trans( transMeta, this );
    trans.setParentJob( getParentJob() );
    if (keepingValues && previousResult!=null) {
      trans.initializeVariablesFrom( previousResult.space );
    } else {
      trans.initializeVariablesFrom( getParentJob() );
    }
    trans.getTransMeta().setInternalKettleVariables( trans );
    trans.injectVariables( getVariablesMap( transMeta ) );

    trans.setLogLevel( getLogLevel() );
    trans.setMetaStore( metaStore );

    // Also copy the parameters over...
    //
    trans.copyParametersFrom( transMeta );
    trans.copyParametersFrom( parentJob );
    transMeta.activateParameters();
    trans.activateParameters();

    // Inform the parent job we started something here...
    //
    for ( DelegationListener delegationListener : parentJob.getDelegationListeners() ) {
      delegationListener.transformationDelegationStarted( trans, new TransExecutionConfiguration() );
    }

    // Run it!
    //
    trans.prepareExecution( parentJob.getArguments() );
    trans.startThreads();
    trans.waitUntilFinished();

    boolean flagSet = trans.getExtensionDataMap().get( REPEAT_END_LOOP ) != null;
    Result result = trans.getResult();
    return new ExecutionResult( result, trans, flagSet );
  }

  private Map<String, String> getVariablesMap( NamedParams namedParams ) {
    Map<String, String> variablesMap = new HashMap<>();
    for ( ParameterDetails parameter : parameters ) {
      try {
        namedParams.getParameterDescription( parameter.getName() );
      } catch ( UnknownParamException e ) {
        variablesMap.put( parameter.getName(), parameter.getName() );
      }
    }
    return variablesMap;
  }

  private ExecutionResult executeJob( String realFilename, int nr, ExecutionResult previousResult ) throws KettleException {

    JobMeta jobMeta = loadJob( realFilename, rep, metaStore, this );

    Job job = new Job( getRepository(), jobMeta, this );
    job.setParentJob( getParentJob() );
    job.setParentVariableSpace( this );
    if (keepingValues && previousResult!=null) {
      job.initializeVariablesFrom( previousResult.space );
    } else {
      job.initializeVariablesFrom( this );
    }
    job.getJobMeta().setInternalKettleVariables( job );
    job.injectVariables( getVariablesMap( jobMeta ) );
    job.setArguments( parentJob.getArguments() );

    job.setLogLevel( getLogLevel() );

    // Also copy the parameters over...
    //
    job.copyParametersFrom( jobMeta );
    job.copyParametersFrom( parentJob );
    jobMeta.activateParameters();
    job.activateParameters();

    if (parentJob.isInteractive()) {
      job.setInteractive( true );
      job.getJobEntryListeners().addAll( parentJob.getJobEntryListeners() );
    }

    // Link the job with the sub-job
    parentJob.getJobTracker().addJobTracker( job.getJobTracker() );
    // Link both ways!
    job.getJobTracker().setParentJobTracker( parentJob.getJobTracker() );

    // Inform the parent job we started something here...
    //
    for ( DelegationListener delegationListener : parentJob.getDelegationListeners() ) {
      delegationListener.jobDelegationStarted( job, new JobExecutionConfiguration() );
    }

    job.start();
    job.waitUntilFinished();

    boolean flagSet = job.getExtensionDataMap().get( REPEAT_END_LOOP ) != null;
    if (flagSet) {
      log.logBasic("End loop flag found, stopping loop.");
    }
    Result result = job.getResult();

    return new ExecutionResult( result, job, flagSet );
  }

  private boolean isVariableValueSet( VariableSpace space ) {

    // See if there's a flag set.
    //
    if (StringUtils.isNotEmpty( variableName )) {
      String realVariable = space.environmentSubstitute( variableName );
      String value = space.getVariable( realVariable );
      if ( StringUtil.isEmpty( value ) ) {
        return false;
      }
      String realValue = environmentSubstitute( variableValue );

      // If we didn't specify any specific value, the variable is simply set.
      //
      if ( StringUtils.isEmpty( realValue ) ) {
        return true;
      }

      // The value in the space and the expected value need to match
      //
      return realValue.equalsIgnoreCase( value );

    }
    return false;
  }


  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();

    xml.append( super.getXML() );

    xml.append( XMLHandler.addTagValue( FILENAME, filename ) );
    xml.append( XMLHandler.addTagValue( VARIABLE_NAME, variableName ) );
    xml.append( XMLHandler.addTagValue( VARIABLE_VALUE, variableValue ) );
    xml.append( XMLHandler.addTagValue( DELAY, delay ) );
    xml.append( XMLHandler.addTagValue( KEEP_VALUES, keepingValues ) );

    xml.append( XMLHandler.openTag( PARAMETERS ) );
    for ( ParameterDetails parameter : parameters ) {
      xml.append( XMLHandler.openTag( PARAMETER ) );
      xml.append( XMLHandler.addTagValue( "name", parameter.getName() ) );
      xml.append( XMLHandler.addTagValue( "value", parameter.getField() ) );
      xml.append( XMLHandler.closeTag( PARAMETER ) );
    }
    xml.append( XMLHandler.closeTag( PARAMETERS ) );

    return xml.toString();
  }

  @Override public void loadXML( Node entryNode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep, IMetaStore metaStore ) throws KettleXMLException {
    super.loadXML( entryNode, databases, slaveServers );

    filename = XMLHandler.getTagValue( entryNode, FILENAME );
    variableName = XMLHandler.getTagValue( entryNode, VARIABLE_NAME );
    variableValue = XMLHandler.getTagValue( entryNode, VARIABLE_VALUE );
    delay = XMLHandler.getTagValue( entryNode, DELAY );
    keepingValues = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, KEEP_VALUES ) );

    Node parametersNode = XMLHandler.getSubNode( entryNode, PARAMETERS );
    List<Node> parameterNodes = XMLHandler.getNodes( parametersNode, PARAMETER );
    parameters = new ArrayList<>();
    for ( Node parameterNode : parameterNodes ) {
      String name = XMLHandler.getTagValue( parameterNode, "name" );
      String field = XMLHandler.getTagValue( parameterNode, "value" );
      parameters.add( new ParameterDetails( name, field ) );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId jobId ) throws KettleException {
    rep.saveJobEntryAttribute( jobId, getObjectId(), FILENAME, filename );
    rep.saveJobEntryAttribute( jobId, getObjectId(), VARIABLE_NAME, variableName );
    rep.saveJobEntryAttribute( jobId, getObjectId(), VARIABLE_VALUE, variableValue );
    rep.saveJobEntryAttribute( jobId, getObjectId(), DELAY, delay );
    rep.saveJobEntryAttribute( jobId, getObjectId(), KEEP_VALUES, keepingValues );

    for ( int i = 0; i < parameters.size(); i++ ) {
      ParameterDetails parameter = parameters.get( i );
      rep.saveJobEntryAttribute( jobId, getObjectId(), i, PARAMETER + "_name", parameters.get( i ).getName() );
      rep.saveJobEntryAttribute( jobId, getObjectId(), i, PARAMETER + "_value", parameters.get( i ).getField() );
    }
  }

  @Override public void loadRep( Repository rep, IMetaStore metaStore, ObjectId jobEntryId, List<DatabaseMeta> databases, List<SlaveServer> slaveServers ) throws KettleException {
    filename = rep.getJobEntryAttributeString( jobEntryId, FILENAME );
    variableName = rep.getJobEntryAttributeString( jobEntryId, VARIABLE_NAME );
    variableValue = rep.getJobEntryAttributeString( jobEntryId, VARIABLE_VALUE );
    delay = rep.getJobEntryAttributeString( jobEntryId, DELAY );
    keepingValues = rep.getJobEntryAttributeBoolean( jobEntryId, KEEP_VALUES );

    parameters = new ArrayList<>();
    int nr = rep.countNrJobEntryAttributes( jobEntryId, PARAMETER + "_name" );
    for ( int i = 0; i < nr; i++ ) {
      String name = rep.getJobEntryAttributeString( jobEntryId, i, PARAMETER + "_name" );
      String field = rep.getJobEntryAttributeString( jobEntryId, i, PARAMETER + "_value" );
      parameters.add( new ParameterDetails( name, field ) );
    }
  }

  @Override public String[] getReferencedObjectDescriptions() {
    String referenceDescription;
    if ( filename.toLowerCase().endsWith( ".ktr" ) ) {
      referenceDescription = "The repeating transformation";
    } else if ( filename.toLowerCase().endsWith( ".kjb" ) ) {
      referenceDescription = "The repeating job";
    } else {
      referenceDescription = "The repeating job or transformation";
    }

    return new String[] {
      referenceDescription
    };
  }

  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { StringUtils.isNotEmpty( filename ) };
  }

  @Override public Object loadReferencedObject( int index, Repository rep, IMetaStore metaStore, VariableSpace space ) throws KettleException {

    String realFilename = space.environmentSubstitute( filename );
    if ( isTransformation( realFilename ) ) {
      return loadTransformation( realFilename, rep, metaStore, space );
    } else if ( isJob( realFilename ) ) {
      return loadJob( realFilename, rep, metaStore, space );
    } else {
      // TODO: open the file, see what's in there.
      //
      throw new KettleException( "Can't tell if this job entry is referencing a transformation or a job" );
    }
  }

  private boolean isTransformation( String realFilename ) throws KettleException {
    if ( realFilename.toLowerCase().endsWith( ".ktr" ) ) {
      return true;
    }
    // See in the file
    try {
      Document document = XMLHandler.loadXMLFile( realFilename );
      return XMLHandler.getSubNode( document, TransMeta.XML_TAG ) != null;
    } catch ( Exception e ) {
      // Not a valid file or XML document
    }
    return false;
  }

  private boolean isJob( String realFilename ) {
    if ( realFilename.toLowerCase().endsWith( ".kjb" ) ) {
      return true;
    }

    // See in the file
    try {
      Document document = XMLHandler.loadXMLFile( realFilename );
      return XMLHandler.getSubNode( document, JobMeta.XML_TAG ) != null;
    } catch ( Exception e ) {
      // Not a valid file or XML document
    }
    return false;
  }

  private TransMeta loadTransformation( String realFilename, Repository rep, IMetaStore metaStore, VariableSpace space ) throws KettleException {
    TransMeta transMeta = new TransMeta( realFilename, metaStore, rep, true, space, null );
    return transMeta;
  }

  private JobMeta loadJob( String realFilename, Repository rep, IMetaStore metaStore, VariableSpace space ) throws KettleException {
    JobMeta jobMeta = new JobMeta( space, realFilename, rep, metaStore, null );
    return jobMeta;
  }

  @Override public String getDialogClassName() {
    return RepeatDialog.class.getName();
  }

  @Override public boolean evaluates() {
    return true;
  }

  @Override public boolean isUnconditional() {
    return false;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  @Override public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
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
   * Gets variableName
   *
   * @return value of variableName
   */
  public String getVariableName() {
    return variableName;
  }

  /**
   * @param variableName The variableName to set
   */
  public void setVariableName( String variableName ) {
    this.variableName = variableName;
  }

  /**
   * Gets variableValue
   *
   * @return value of variableValue
   */
  public String getVariableValue() {
    return variableValue;
  }

  /**
   * @param variableValue The variableValue to set
   */
  public void setVariableValue( String variableValue ) {
    this.variableValue = variableValue;
  }

  /**
   * Gets delay
   *
   * @return value of delay
   */
  public String getDelay() {
    return delay;
  }

  /**
   * @param delay The delay to set
   */
  public void setDelay( String delay ) {
    this.delay = delay;
  }

  /**
   * Gets keepingValues
   *
   * @return value of keepingValues
   */
  public boolean isKeepingValues() {
    return keepingValues;
  }

  /**
   * @param keepingValues The keepingValues to set
   */
  public void setKeepingValues( boolean keepingValues ) {
    this.keepingValues = keepingValues;
  }
}
