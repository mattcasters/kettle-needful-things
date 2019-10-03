package org.pentaho.di.needful.entries.repeat;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannelFileWriter;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.DelegationListener;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
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

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

  public static final String LOGFILE_ENABLED = "logfile_enabled";
  public static final String LOGFILE_APPENDED = "logfile_appended";
  public static final String LOGFILE_BASE = "logfile_base";
  public static final String LOGFILE_EXTENSION = "logfile_extension";
  public static final String LOGFILE_ADD_DATE = "logfile_add_date";
  public static final String LOGFILE_ADD_TIME = "logfile_add_time";
  public static final String LOGFILE_ADD_REPETITION = "logfile_add_repetition";
  public static final String LOGFILE_UPDATE_INTERVAL = "logfile_update_interval";

  public static final String PARAMETERS = "parameters";
  public static final String PARAMETER = "parameter";

  private String filename;
  private List<ParameterDetails> parameters;
  private String variableName;
  private String variableValue;
  private String delay;
  private boolean keepingValues;

  // Here is a list of options to log to a file
  //
  private boolean logFileEnabled;
  private String logFileBase;
  private String logFileExtension = "log";
  private boolean logFileAppended = true;
  private boolean logFileDateAdded = true;
  private boolean logFileTimeAdded = false;
  private boolean logFileRepetitionAdded = false;
  private String logFileUpdateInterval = "5000";

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

  public Repeat( String name, String description ) {
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

    if ( StringUtils.isNotEmpty( delay ) ) {
      String realDelay = environmentSubstitute( delay );
      delayInMs = Const.toLong( realDelay, -1 );
      if ( delayInMs < 0 ) {
        throw new KettleException( "Unable to parse delay for string: " + realDelay );
      }
      delayInMs *= 1000; // convert to ms
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
    int repetitionNr = 0;
    while ( repeat && !parentJob.isStopped() ) {
      repetitionNr++;
      executionResult = executeTransformationOrJob( realFilename, nr, executionResult, repetitionNr );
      Result result = executionResult.result;
      if ( !result.getResult() || result.getNrErrors() > 0 || result.isStopped() ) {
        log.logError( "The repeating work encountered and error or was stopped. This ends the loop." );

        // On an false result, stop the loop
        //
        prevResult.setResult( false );

        repeat = false;
      } else {
        // End repeat if the End Repeat job entry is executed
        //
        if ( executionResult.flagSet ) {
          repeat = false;
        } else {
          // Repeat as long as the variable is not set.
          //
          repeat = !isVariableValueSet( executionResult.space );
        }
      }

      if ( repeat && delayInMs > 0 ) {
        // See if we need to delay
        //
        long startTime = System.currentTimeMillis();
        while ( !parentJob.isStopped() && ( System.currentTimeMillis() - startTime < delayInMs ) ) {
          try {
            Thread.sleep( 100 );
          } catch ( Exception e ) {
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
    JobEntryTrans jeTrans = new JobEntryTrans( getName() );
    JobEntryCopy key = new JobEntryCopy( jeTrans );
    parentJob.getActiveJobEntryTransformations().put( key, jeTrans );
  }

  private void clearBusyIcon() {
    // Remove the busy icon in the graph
    //
    JobEntryTrans jeTrans = new JobEntryTrans( getName() );
    JobEntryCopy key = new JobEntryCopy( jeTrans );
    parentJob.getActiveJobEntryTransformations().remove( key );
  }

  private ExecutionResult executeTransformationOrJob( String realFilename, int nr, ExecutionResult previousResult, int repetitionNr ) throws KettleException {
    if ( isTransformation( realFilename ) ) {
      return executeTransformation( realFilename, nr, previousResult, repetitionNr );
    }
    if ( isJob( realFilename ) ) {
      return executeJob( realFilename, nr, previousResult, repetitionNr );
    }
    throw new KettleException( "Don't know if this is a transformation or a job" );
  }

  private ExecutionResult executeTransformation( String realFilename, int nr, ExecutionResult previousResult, int repetitionNr ) throws KettleException {
    TransMeta transMeta = loadTransformation( realFilename, rep, metaStore, this );
    Trans trans = new Trans( transMeta, this );
    trans.setParentJob( getParentJob() );
    if ( keepingValues && previousResult != null ) {
      trans.copyVariablesFrom( previousResult.space );
    } else {
      trans.initializeVariablesFrom( getParentJob() );

      // Also copy the parameters over...
      //
      trans.copyParametersFrom( transMeta );
      trans.copyParametersFrom( parentJob );

    }
    trans.getTransMeta().setInternalKettleVariables( trans );
    trans.injectVariables( getVariablesMap( transMeta, previousResult ) );

    NamedParams previousParams = previousResult == null ? null : (NamedParams) previousResult.space;
    VariableSpace previousVars = previousResult == null ? null : previousResult.space;
    updateParameters( trans, previousVars, getParentJob(), previousParams );
    updateParameters( transMeta, previousVars, getParentJob(), previousParams );

    trans.setLogLevel( getLogLevel() );
    trans.setMetaStore( metaStore );

    // Inform the parent job we started something here...
    //
    for ( DelegationListener delegationListener : parentJob.getDelegationListeners() ) {
      delegationListener.transformationDelegationStarted( trans, new TransExecutionConfiguration() );
    }

    // Start logging before execution...
    //
    LogChannelFileWriter fileWriter = null;
    try {
      if ( logFileEnabled ) {
        fileWriter = logToFile( trans, repetitionNr );
      }

      // Run it!
      //
      trans.prepareExecution( parentJob.getArguments() );
      trans.startThreads();
      trans.waitUntilFinished();

      boolean flagSet = trans.getExtensionDataMap().get( REPEAT_END_LOOP ) != null;
      Result result = trans.getResult();
      return new ExecutionResult( result, trans, flagSet );
    } finally {
      if (logFileEnabled && fileWriter!=null) {
        fileWriter.stopLogging();
      }
    }
  }

  private LogChannelFileWriter logToFile( LoggingObjectInterface loggingObject, int repetitionNr ) throws KettleException {

    // Calculate the filename
    //
    Date currentDate = new Date();

    String filename = environmentSubstitute( logFileBase );
    if (logFileDateAdded) {
      filename+="_"+new SimpleDateFormat( "yyyyMMdd" ).format( currentDate );
    }
    if (logFileTimeAdded) {
      filename+="_"+new SimpleDateFormat( "HHmmss" ).format( currentDate );
    }
    if (logFileRepetitionAdded) {
      filename+="_"+new DecimalFormat("0000").format( repetitionNr );
    }
    filename+="."+environmentSubstitute( logFileExtension );


    String logChannelId = loggingObject.getLogChannelId();
    LogChannelFileWriter fileWriter = new LogChannelFileWriter( logChannelId, KettleVFS.getFileObject( filename ), logFileAppended, Const.toInt( logFileUpdateInterval, 5000) );

    fileWriter.startLogging();

    return fileWriter;

  }

  private Map<String, String> getVariablesMap( NamedParams namedParams, ExecutionResult previousResult ) {
    String[] params = namedParams.listParameters();
    Map<String, String> variablesMap = new HashMap<>();

    if ( keepingValues && previousResult != null ) {
      for ( String variableName : previousResult.space.listVariables() ) {
        variablesMap.put( variableName, previousResult.space.getVariable( variableName ) );
      }
    } else {
      // Initialize the values of the defined parameters in the job entry
      //
      for ( ParameterDetails parameter : parameters ) {
        String value = environmentSubstitute( parameter.getField() );
        variablesMap.put( parameter.getName(), value );
      }
    }
    return variablesMap;
  }

  private ExecutionResult executeJob( String realFilename, int nr, ExecutionResult previousResult, int repetitionNr ) throws KettleException {

    JobMeta jobMeta = loadJob( realFilename, rep, metaStore, this );
    Job job = new Job( getRepository(), jobMeta, this );
    job.setParentJob( getParentJob() );
    job.setParentVariableSpace( this );
    if ( keepingValues && previousResult != null ) {
      job.copyVariablesFrom( previousResult.space );
    } else {
      job.initializeVariablesFrom( this );

      // Also copy the parameters over...
      //
      job.copyParametersFrom( jobMeta );
      job.copyParametersFrom( parentJob );
    }

    job.getJobMeta().setInternalKettleVariables( job );
    job.injectVariables( getVariablesMap( jobMeta, previousResult ) );

    NamedParams previousParams = previousResult == null ? null : (NamedParams) previousResult.space;
    VariableSpace previousVars = previousResult == null ? null : (VariableSpace) previousResult.space;
    updateParameters( job, previousVars, getParentJob(), previousParams );
    updateParameters( jobMeta, previousVars, getParentJob(), previousParams );
    job.setArguments( parentJob.getArguments() );

    job.setLogLevel( getLogLevel() );

    if ( parentJob.isInteractive() ) {
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

    // Start logging before execution...
    //
    LogChannelFileWriter fileWriter = null;
    try {
      if ( logFileEnabled ) {
        fileWriter = logToFile( job, repetitionNr );
      }

      job.start();
      job.waitUntilFinished();

      boolean flagSet = job.getExtensionDataMap().get( REPEAT_END_LOOP ) != null;
      if ( flagSet ) {
        log.logBasic( "End loop flag found, stopping loop." );
      }
      Result result = job.getResult();

      return new ExecutionResult( result, job, flagSet );
    } finally {
      if (logFileEnabled && fileWriter!=null) {
        fileWriter.stopLogging();
      }
    }
  }

  private void updateParameters( NamedParams subParams, VariableSpace subVars, NamedParams... params ) {
    // Inherit
    for ( NamedParams param : params ) {
      if ( param != null ) {
        subParams.mergeParametersWith( param, true );
      }
    }

    // Any parameters to initialize from the job entry?
    //
    String[] parameterNames = subParams.listParameters();
    for ( ParameterDetails parameter : parameters ) {
      if ( Const.indexOfString( parameter.getName(), parameterNames ) >= 0 ) {
        // Set this parameter
        //
        String value = environmentSubstitute( parameter.getField() );
        try {
          subParams.setParameterValue( parameter.getName(), value );
        } catch ( UnknownParamException e ) {
          // Ignore
        }
      }
    }

    // Changed values?
    //
    if ( keepingValues && subVars != null ) {
      for ( String parameterName : subParams.listParameters() ) {
        try {
          String value = subVars.getVariable( parameterName );
          subParams.setParameterValue( parameterName, value );
        } catch ( UnknownParamException e ) {
          // Ignore
        }
      }
    }

    subParams.activateParameters();
  }

  private boolean isVariableValueSet( VariableSpace space ) {

    // See if there's a flag set.
    //
    if ( StringUtils.isNotEmpty( variableName ) ) {
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

    xml.append( XMLHandler.addTagValue( LOGFILE_ENABLED, logFileEnabled ) );
    xml.append( XMLHandler.addTagValue( LOGFILE_APPENDED, logFileAppended ) );
    xml.append( XMLHandler.addTagValue( LOGFILE_BASE, logFileBase ) );
    xml.append( XMLHandler.addTagValue( LOGFILE_EXTENSION, logFileExtension ) );
    xml.append( XMLHandler.addTagValue( LOGFILE_ADD_DATE, logFileDateAdded ) );
    xml.append( XMLHandler.addTagValue( LOGFILE_ADD_TIME, logFileTimeAdded ) );
    xml.append( XMLHandler.addTagValue( LOGFILE_ADD_REPETITION, logFileRepetitionAdded ) );
    xml.append( XMLHandler.addTagValue( LOGFILE_UPDATE_INTERVAL, logFileUpdateInterval ) );

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

    logFileEnabled = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, LOGFILE_ENABLED ) );
    logFileAppended = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, LOGFILE_APPENDED ) );
    logFileDateAdded = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, LOGFILE_ADD_DATE ) );
    logFileTimeAdded = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, LOGFILE_ADD_TIME ) );
    logFileRepetitionAdded = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, LOGFILE_ADD_REPETITION ) );
    logFileBase = XMLHandler.getTagValue( entryNode, LOGFILE_BASE );
    logFileExtension = XMLHandler.getTagValue( entryNode, LOGFILE_EXTENSION );
    logFileUpdateInterval = XMLHandler.getTagValue( entryNode, LOGFILE_UPDATE_INTERVAL );

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

    rep.saveJobEntryAttribute( jobId, getObjectId(), LOGFILE_ENABLED, logFileEnabled );
    rep.saveJobEntryAttribute( jobId, getObjectId(), LOGFILE_APPENDED, logFileAppended );
    rep.saveJobEntryAttribute( jobId, getObjectId(), LOGFILE_BASE, logFileBase );
    rep.saveJobEntryAttribute( jobId, getObjectId(), LOGFILE_EXTENSION, logFileExtension );
    rep.saveJobEntryAttribute( jobId, getObjectId(), LOGFILE_ADD_DATE, logFileDateAdded );
    rep.saveJobEntryAttribute( jobId, getObjectId(), LOGFILE_ADD_TIME, logFileTimeAdded );
    rep.saveJobEntryAttribute( jobId, getObjectId(), LOGFILE_ADD_REPETITION, logFileRepetitionAdded );
    rep.saveJobEntryAttribute( jobId, getObjectId(), LOGFILE_UPDATE_INTERVAL, logFileUpdateInterval );

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

    logFileEnabled = rep.getJobEntryAttributeBoolean( jobEntryId, LOGFILE_ENABLED );
    logFileAppended = rep.getJobEntryAttributeBoolean( jobEntryId, LOGFILE_APPENDED );
    logFileBase = rep.getJobEntryAttributeString( jobEntryId, LOGFILE_BASE );
    logFileExtension = rep.getJobEntryAttributeString( jobEntryId, LOGFILE_EXTENSION );
    logFileDateAdded = rep.getJobEntryAttributeBoolean( jobEntryId, LOGFILE_ADD_DATE );
    logFileTimeAdded = rep.getJobEntryAttributeBoolean( jobEntryId, LOGFILE_ADD_TIME );
    logFileRepetitionAdded = rep.getJobEntryAttributeBoolean( jobEntryId, LOGFILE_ADD_REPETITION );
    logFileUpdateInterval = rep.getJobEntryAttributeString( jobEntryId, LOGFILE_UPDATE_INTERVAL );

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

  /**
   * Gets logFileEnabled
   *
   * @return value of logFileEnabled
   */
  public boolean isLogFileEnabled() {
    return logFileEnabled;
  }

  /**
   * @param logFileEnabled The logFileEnabled to set
   */
  public void setLogFileEnabled( boolean logFileEnabled ) {
    this.logFileEnabled = logFileEnabled;
  }

  /**
   * Gets logFileBase
   *
   * @return value of logFileBase
   */
  public String getLogFileBase() {
    return logFileBase;
  }

  /**
   * @param logFileBase The logFileBase to set
   */
  public void setLogFileBase( String logFileBase ) {
    this.logFileBase = logFileBase;
  }

  /**
   * Gets logFileExtension
   *
   * @return value of logFileExtension
   */
  public String getLogFileExtension() {
    return logFileExtension;
  }

  /**
   * @param logFileExtension The logFileExtension to set
   */
  public void setLogFileExtension( String logFileExtension ) {
    this.logFileExtension = logFileExtension;
  }

  /**
   * Gets logFileAppended
   *
   * @return value of logFileAppended
   */
  public boolean isLogFileAppended() {
    return logFileAppended;
  }

  /**
   * @param logFileAppended The logFileAppended to set
   */
  public void setLogFileAppended( boolean logFileAppended ) {
    this.logFileAppended = logFileAppended;
  }

  /**
   * Gets logFileDateAdded
   *
   * @return value of logFileDateAdded
   */
  public boolean isLogFileDateAdded() {
    return logFileDateAdded;
  }

  /**
   * @param logFileDateAdded The logFileDateAdded to set
   */
  public void setLogFileDateAdded( boolean logFileDateAdded ) {
    this.logFileDateAdded = logFileDateAdded;
  }

  /**
   * Gets logFileTimeAdded
   *
   * @return value of logFileTimeAdded
   */
  public boolean isLogFileTimeAdded() {
    return logFileTimeAdded;
  }

  /**
   * @param logFileTimeAdded The logFileTimeAdded to set
   */
  public void setLogFileTimeAdded( boolean logFileTimeAdded ) {
    this.logFileTimeAdded = logFileTimeAdded;
  }

  /**
   * Gets logFileRepetitionAdded
   *
   * @return value of logFileRepetitionAdded
   */
  public boolean isLogFileRepetitionAdded() {
    return logFileRepetitionAdded;
  }

  /**
   * @param logFileRepetitionAdded The logFileRepetitionAdded to set
   */
  public void setLogFileRepetitionAdded( boolean logFileRepetitionAdded ) {
    this.logFileRepetitionAdded = logFileRepetitionAdded;
  }

  /**
   * Gets logFileUpdateInterval
   *
   * @return value of logFileUpdateInterval
   */
  public String getLogFileUpdateInterval() {
    return logFileUpdateInterval;
  }

  /**
   * @param logFileUpdateInterval The logFileUpdateInterval to set
   */
  public void setLogFileUpdateInterval( String logFileUpdateInterval ) {
    this.logFileUpdateInterval = logFileUpdateInterval;
  }

}
