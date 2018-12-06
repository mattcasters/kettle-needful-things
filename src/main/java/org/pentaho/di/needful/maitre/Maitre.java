package org.pentaho.di.needful.maitre;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.ExecutionConfiguration;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.extension.ExtensionPointHandler;
import org.pentaho.di.core.extension.KettleExtensionPoint;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.needful.RunConfiguration;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.cluster.TransSplitter;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerTransStatus;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;
import picocli.CommandLine;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Maitre implements Runnable {
  public static final String MAITRE_START = "MaitreStart";

  @Option( names = { "-z", "--file" }, description = "The filename of the job or transformation to run" )
  private String filename;

  @Option( names = { "-l", "--level" }, description = "The debug level, one of NONE, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL" )
  private String level;

  @Option( names = { "-h", "--help" }, usageHelp = true, description = "Displays this help message and quits." )
  private boolean helpRequested;

  @Option( names = { "-p", "--parameters" }, description = "A comma separated list of PARAMTER=VALUE pairs", split = "," )
  private String[] parameters = null;

  @Option( names = { "-r", "--runconfig" }, description = "The name of the Run Configuration to use" )
  private String runConfigurationName = null;

  @Option( names = { "-t", "--transformation" }, description = "Force execution of a transformation" )
  private boolean runTransformation = false;

  @Option( names = { "-j", "--job" }, description = "Force execution of a job" )
  private boolean runJob = false;

  @Option( names = { "-a", "--safemode" }, description = "Run in safe mode" )
  private boolean safeMode = false;

  @Option( names = { "-m", "--metrics" }, description = "Gather metrics" )
  private boolean gatherMetrics = false;

  @Option( names = { "-s", "--slave" }, description = "The slave server to run on" )
  private String slaveServerName;

  @Option( names = { "-x", "--export" }, description = "Export all resources and send them to the slave server" )
  private boolean exportToSlaveServer = false;

  @Option( names = { "-c", "--clustered" }, description = "Execute clustered on the specified slave server (the master)" )
  private boolean executeClustered = false;

  @Option( names = { "-q", "--querydelay" }, description = "Delay between querying of remote servers" )
  private String queryDelay;

  @Option( names = { "-d", "--dontwait" }, description = "Do not wait until the remote job or transformation is done" )
  private boolean dontWait = false;

  @Option( names = { "-g", "--remotelog" }, description = "Write out the remote log of remote executions" )
  private boolean remoteLogging = false;

  @Option( names = { "-o", "--printoptions" }, description = "Print the used options" )
  private boolean printingOptions = false;

  @Option( names = { "-initialDir" }, description = "Ignored", hidden = true )
  private String intialDir = null;

  @Parameters(arity = "1..*", paramLabel = "ARGUMENT", description = "Command line argument(s)", defaultValue = "", hidden = true)
  private String[] arguments;

  @Option( names = { "-e", "--environment" }, description = "The name of the environment to use" )
  private String environment;


  private VariableSpace space;
  private String realRunConfigurationName;
  private String realFilename;
  private String realSlaveServerName;
  private CommandLine cmd;
  private LogChannel log;
  private DelegatingMetaStore metaStore;

  public void run() {
    validateOptions();

    try {
      initialize( cmd );

      log = new LogChannel( "Maitre" );
      log.logDetailed( "Start of Maitre" );

      // Allow modification of various environment settings
      //
      ExtensionPointHandler.callExtensionPoint( log, MAITRE_START, environment );

      buildVariableSpace();
      buildMetaStore();



      if ( isTransformation() ) {
        runTransformation( cmd, log );
      }
      if ( isJob() ) {
        runJob( cmd, log );
      }
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of file '" + filename + "'", e );
    }
  }

  private void initialize( CommandLine cmd ) {
    try {
      KettleEnvironment.init();
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was a problem during the initialization of the Kettle environment", e );
    }
  }

  private void buildVariableSpace() throws IOException {
    // Load kettle.properties before running for convenience...
    //
    space = new Variables();
    space.initializeVariablesFrom( null );
    Properties kettleProperties = new Properties();
    kettleProperties.load( new FileInputStream( Const.getKettleDirectory() + "/kettle.properties" ) );
    for ( final String key : kettleProperties.stringPropertyNames() ) {
      space.setVariable( key, kettleProperties.getProperty( key ) );
    }
  }

  private void runTransformation( CommandLine cmd, LogChannelInterface log ) {

    try {
      calculateRealFilename();

      // Run the transformation with the given filename
      //
      TransMeta transMeta = new TransMeta( realFilename, true );
      transMeta.setMetaStore( metaStore );

      // Configure the basic execution settings
      //
      TransExecutionConfiguration configuration = new TransExecutionConfiguration();

      // Copy run config details from the metastore over to the run configuration
      //
      parseRunConfiguration( cmd, configuration, metaStore, transMeta.getSharedObjects() );

      // Overwrite if the user decided this
      //
      parseOptions( cmd, configuration, transMeta.getSharedObjects(), transMeta );

      // Trans specific
      //
      configuration.setExecutingClustered( executeClustered );

      // configure the variables and parameters
      //
      configureParametersAndVariables( cmd, configuration, transMeta, transMeta );

      // Certain Pentaho plugins rely on this.  Meh.
      //
      ExtensionPointHandler.callExtensionPoint( log, KettleExtensionPoint.SpoonTransBeforeStart.id, new Object[] {
        configuration, null, transMeta, null } );

      // Before running, do we print the options?
      //
      if ( printingOptions ) {
        printOptions( configuration );
      }

      // Now run the transformation
      //
      if ( configuration.isExecutingLocally() ) {
        runTransLocal( cmd, log, configuration, transMeta );
      } else if ( configuration.isExecutingRemotely() ) {
        if ( configuration.isExecutingClustered() ) {
          runTransClustered( cmd, log, transMeta, configuration );
        } else {
          // Simply remote execution.
          //
          runTransRemote( cmd, log, transMeta, configuration );
        }
      }

    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of transformation '" + filename + "'", e );
    }
  }

  /**
   * This way we can actually use environment variables to parse the real filename
   */
  private void calculateRealFilename() {
    realFilename = space.environmentSubstitute( filename );
  }

  private void runTransLocal( CommandLine cmd, LogChannelInterface log, TransExecutionConfiguration configuration, TransMeta transMeta ) {
    try {
      Trans trans = new Trans( transMeta );
      trans.initializeVariablesFrom( null );
      trans.getTransMeta().setInternalKettleVariables( trans );
      trans.injectVariables( configuration.getVariables() );

      trans.setLogLevel( configuration.getLogLevel() );
      trans.setMetaStore( metaStore );

      // Also copy the parameters over...
      //
      trans.copyParametersFrom( transMeta );
      transMeta.activateParameters();
      trans.activateParameters();

      // Run it!
      //
      trans.prepareExecution( arguments );
      trans.startThreads();
      trans.waitUntilFinished();
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "Error running transformation locally", e );
    }
  }

  private void runTransRemote( CommandLine cmd, LogChannelInterface log, TransMeta transMeta, TransExecutionConfiguration configuration ) {
    SlaveServer slaveServer = configuration.getRemoteServer();
    slaveServer.shareVariablesWith( transMeta );
    try {
      runTransformationOnSlaveServer( log, transMeta, slaveServer, configuration, metaStore, dontWait, getQueryDelay() );
    } catch(Exception e) {
      throw new ExecutionException( cmd, e.getMessage(), e );
    }
  }

  public static Result runTransformationOnSlaveServer( LogChannelInterface log, TransMeta transMeta, SlaveServer slaveServer, TransExecutionConfiguration configuration, IMetaStore metaStore, boolean dontWait, int queryDelay ) throws Exception {
    try {
      String carteObjectId = Trans.sendToSlaveServer( transMeta, configuration, null, metaStore );
      if (!dontWait) {
        Trans.monitorRemoteTransformation( log, carteObjectId, transMeta.getName(), slaveServer, queryDelay );
        SlaveServerTransStatus transStatus = slaveServer.getTransStatus( transMeta.getName(), carteObjectId, 0 );
        if ( configuration.isLogRemoteExecutionLocally() ) {
          log.logBasic( transStatus.getLoggingString() );
        }
        if ( transStatus.getNrStepErrors() > 0 ) {
          // Error
          throw new Exception( "Remote transformation ended with an error" );
        }

        return transStatus.getResult();
      }
      return null; // No status, we don't wait for it.
    } catch ( Exception e ) {
      throw new Exception( "Error executing transformation remotely on server '" + slaveServer.getName() + "'", e );
    }
  }

  private void runTransClustered( CommandLine cmd, LogChannelInterface log, TransMeta transMeta, TransExecutionConfiguration configuration ) throws ExecutionException {
    try {
      TransSplitter transSplitter = Trans.executeClustered( transMeta, configuration );
      long errors = Trans.monitorClusteredTransformation( log, transSplitter, null, getQueryDelay() );
      Result clusteredResult = Trans.getClusteredTransformationResult( log, transSplitter, null, configuration.isLogRemoteExecutionLocally() );

      if (configuration.isLogRemoteExecutionLocally()) {
        log.logBasic( clusteredResult.getLogText() );
      }
      if ( !clusteredResult.getResult() ) {
        // Error
        throw new ExecutionException( cmd, "Clustered transformation ended with an error" );
      }
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "Error executing clustered transformation", e );
    }
  }

  private void runJob( CommandLine cmd, LogChannelInterface log ) {
    try {
      calculateRealFilename();

      // Run the job with the given filename
      //
      JobMeta jobMeta = new JobMeta( realFilename, null, null );
      jobMeta.setMetaStore( metaStore );

      // Configure the basic execution settings
      //
      JobExecutionConfiguration configuration = new JobExecutionConfiguration();

      // Copy run config details from the metastore over to the run configuration
      //
      parseRunConfiguration( cmd, configuration, metaStore, jobMeta.getSharedObjects() );

      // Overwrite the run configuration with optional command line options
      //
      parseOptions( cmd, configuration, jobMeta.getSharedObjects(), jobMeta );

      // Certain Pentaho plugins rely on this.  Meh.
      //
      ExtensionPointHandler.callExtensionPoint( log, KettleExtensionPoint.SpoonTransBeforeStart.id, new Object[] {
        configuration, null, jobMeta, null } );

      // Before running, do we print the options?
      //
      if ( printingOptions ) {
        printOptions( configuration );
      }

      // Now we can run the job
      //
      if ( configuration.isExecutingLocally() ) {
        runJobLocal( cmd, log, configuration, jobMeta );
      } else if ( configuration.isExecutingRemotely() ) {
        // Simply remote execution.
        //
        runJobRemote( cmd, log, jobMeta, configuration );
      }


    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of job '" + filename + "'", e );
    }
  }

  private void runJobLocal( CommandLine cmd, LogChannelInterface log, JobExecutionConfiguration configuration, JobMeta jobMeta ) {
    try {
      Job job = new Job( null, jobMeta );
      job.initializeVariablesFrom( null );
      job.getJobMeta().setInternalKettleVariables( job );
      job.injectVariables( configuration.getVariables() );
      job.setArguments( arguments );

      job.setLogLevel( configuration.getLogLevel() );

      // Also copy the parameters over...
      //
      job.copyParametersFrom( jobMeta );
      jobMeta.activateParameters();
      job.activateParameters();

      job.start();
      job.waitUntilFinished();
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "Error running job locally", e );
    }
  }

  private void runJobRemote( CommandLine cmd, LogChannelInterface log, JobMeta jobMeta, JobExecutionConfiguration configuration ) {
    SlaveServer slaveServer = configuration.getRemoteServer();
    slaveServer.shareVariablesWith( jobMeta );

    try {

    } catch(Exception e) {
      throw new ExecutionException( cmd, e.getMessage(), e );
    }
  }

  public static Result runJobOnSlaveServer(LogChannelInterface log, JobMeta jobMeta, SlaveServer slaveServer, JobExecutionConfiguration configuration, IMetaStore metaStore, boolean dontWait, boolean remoteLogging, int queryDelay ) throws Exception {
    try {
      String carteObjectId = Job.sendToSlaveServer( jobMeta, configuration, null, metaStore );

      // Monitor until finished...
      //
      SlaveServerJobStatus jobStatus = null;
      Result oneResult = new Result();

      while ( !dontWait ) {
        try {
          jobStatus = slaveServer.getJobStatus( jobMeta.getName(), carteObjectId, 0 );
          if ( jobStatus.getResult() != null ) {
            // The job is finished, get the result...
            //
            oneResult = jobStatus.getResult();
            break;
          }
        } catch ( Exception e1 ) {
          log.logError( "Unable to contact slave server [" + slaveServer + "] to verify the status of job [" + jobMeta.getName() + "]", e1 );
          oneResult.setNrErrors( 1L );
          break; // Stop looking too, chances are too low the server will
          // come back on-line
        }

        // sleep for a while
        try {
          Thread.sleep( queryDelay );
        } catch ( InterruptedException e ) {
          // Ignore
        }
      }

      // Get the status
      //
      if (!dontWait) {
        jobStatus = slaveServer.getJobStatus( jobMeta.getName(), carteObjectId, 0 );
        if ( remoteLogging ) {
          log.logBasic( jobStatus.getLoggingString() );
        }
        Result result = jobStatus.getResult();
        if ( result.getNrErrors() > 0 ) {
          // Error
          throw new Exception( "Remote job ended with an error" );
        }
        return result;
      }
      return null;
    } catch ( Exception e ) {
      throw new Exception( "Error executing job remotely on server '" + slaveServer.getName() + "'", e );
    }
  }

  private int getQueryDelay() {
    if ( StringUtils.isEmpty( queryDelay ) ) {
      return 5;
    }
    return Const.toInt( queryDelay, 5 );
  }

  private void parseOptions( CommandLine cmd, ExecutionConfiguration configuration, SharedObjects sharedObjects, NamedParams namedParams ) {

    configuration.setSafeModeEnabled( safeMode );
    configuration.setGatheringMetrics( gatherMetrics );

    if ( StringUtils.isNotEmpty( slaveServerName ) ) {
      realSlaveServerName = space.environmentSubstitute( slaveServerName );
      configureSlaveServer( configuration, sharedObjects, realSlaveServerName );
      configuration.setExecutingRemotely( true );
      configuration.setExecutingLocally( false );
    }
    configuration.setPassingExport( exportToSlaveServer );
    realRunConfigurationName = space.environmentSubstitute( runConfigurationName );
    configuration.setRunConfiguration( realRunConfigurationName );
    configuration.setLogLevel( LogLevel.getLogLevelForCode( space.environmentSubstitute( level ) ) );

    // Set variables and parameters...
    //
    parseParametersAndVariables( cmd, configuration, namedParams );
  }

  private void configureSlaveServer( ExecutionConfiguration configuration, SharedObjects sharedObjects, String name ) {
    SlaveServer slaveServer = (SlaveServer) sharedObjects.getSharedObject( SlaveServer.class.getName(), name );
    if (slaveServer==null) {
      throw new ParameterException( cmd, "Unable to find shared slave server '"+name+"'" );
    }
    configuration.setRemoteServer( slaveServer );
  }

  private boolean isTransformation() {
    if ( runTransformation ) {
      return true;
    }
    return filename.toLowerCase().endsWith( ".ktr" );
  }

  private boolean isJob() {
    if ( runJob ) {
      return true;
    }
    return filename.toLowerCase().endsWith( ".kjb" );
  }

  private void parseRunConfiguration( CommandLine cmd, ExecutionConfiguration configuration,
                                      IMetaStore metaStore, SharedObjects sharedObjects ) throws ParameterException {
    realRunConfigurationName = space.environmentSubstitute( runConfigurationName );
    if ( StringUtils.isEmpty( realRunConfigurationName ) ) {
      return;
    }
    try {
      MetaStoreFactory<RunConfiguration> configFactory = new MetaStoreFactory<>( RunConfiguration.class, metaStore, PentahoDefaults.NAMESPACE );
      RunConfiguration runConfiguration = configFactory.loadElement( realRunConfigurationName );
      if ( runConfiguration == null ) {
        // Not found!
        throw new ParameterException( cmd, "Run configuration '" + realRunConfigurationName + "' couldn't be found" );
      }

      // Handle details
      //
      if ( StringUtils.isNotEmpty( runConfiguration.getServer() ) ) {
        realSlaveServerName = space.environmentSubstitute( runConfiguration.getServer() );
        configureSlaveServer( configuration, sharedObjects, realSlaveServerName );
      }
      configuration.setExecutingLocally( runConfiguration.isLocal() );
      configuration.setExecutingRemotely( runConfiguration.isRemote() );
      configuration.setPassingExport( runConfiguration.isSendResources() );

      // Trans specific
      //
      if ( configuration instanceof TransExecutionConfiguration ) {
        ( (TransExecutionConfiguration) configuration ).setExecutingClustered( runConfiguration.isClustered() );
      }

      // Job specific
      //
      if ( configuration instanceof JobExecutionConfiguration ) {
        // Nothing so far
      }

    } catch ( Exception e ) {
      throw new ParameterException( cmd, "Unable to load run configuration '" + realRunConfigurationName + "'", e );
    }
  }


  /**
   * Set the variables and parameters
   *
   * @param cmd
   * @param configuration
   * @param namedParams
   */
  private void parseParametersAndVariables( CommandLine cmd, ExecutionConfiguration configuration, NamedParams namedParams ) {
    try {
      String[] availableParameters = namedParams.listParameters();
      if ( parameters != null ) {
        for ( String parameter : parameters ) {
          String[] split = parameter.split( "=" );
          String key = split.length > 0 ? split[ 0 ] : null;
          String value = split.length > 1 ? split[ 1 ] : null;

          if ( key != null ) {
            // We can work with this.
            //
            if ( Const.indexOfString( key, availableParameters ) < 0 ) {
              // A variable
              //
              configuration.getVariables().put( key, value );
            } else {
              // A parameter
              //
              configuration.getParams().put( key, value );
            }
          }
        }
      }
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of transformation '" + filename + "'", e );
    }
  }

  private void buildMetaStore() throws MetaStoreException {
    metaStore = new DelegatingMetaStore();
    IMetaStore localMetaStore = MetaStoreConst.openLocalPentahoMetaStore();
    metaStore.addMetaStore( localMetaStore );
    metaStore.setActiveMetaStoreName( localMetaStore.getName() );
  }


  /**
   * Configure the variables and parameters in the given configuration on the given variable space and named parameters
   *
   * @param cmd
   * @param configuration
   * @param namedParams
   */
  private void configureParametersAndVariables( CommandLine cmd, ExecutionConfiguration configuration, VariableSpace space, NamedParams namedParams ) {

    // Copy variables over to the transformation or job metadata
    //
    space.injectVariables( configuration.getVariables() );

    // Set the parameter values
    //
    for ( String key : configuration.getParams().keySet() ) {
      String value = configuration.getParams().get( key );
      try {
        namedParams.setParameterValue( key, value );
      } catch ( UnknownParamException e ) {
        throw new ParameterException( cmd, "Unable to set parameter '" + key + "'", e );
      }
    }
  }

  private void validateOptions() {
    if ( StringUtils.isEmpty( filename ) ) {
      throw new ParameterException( new CommandLine( this ), "A filename is needed to run a job or transformation" );
    }
  }

  private void printOptions( ExecutionConfiguration configuration ) {
    if ( StringUtils.isNotEmpty( realFilename ) ) {
      log.logMinimal( "OPTION: filename : '" + realFilename + "'" );
    }
    if ( StringUtils.isNotEmpty( realRunConfigurationName ) ) {
      log.logMinimal( "OPTION: run configuration : '" + realRunConfigurationName + "'" );
    }
    if ( StringUtils.isNotEmpty( realSlaveServerName ) ) {
      log.logMinimal( "OPTION: slave server: '" + realSlaveServerName + "'" );
    }
    // Where are we executing? Local, Remote, Clustered?
    if ( configuration.isExecutingLocally() ) {
      log.logMinimal( "OPTION: Local execution" );
    } else {
      if ( configuration.isExecutingRemotely() ) {
        log.logMinimal( "OPTION: Remote execution" );

        // Clustered?
        if ( configuration instanceof TransExecutionConfiguration ) {
          if ( ( (TransExecutionConfiguration) configuration ).isExecutingClustered() ) {
            log.logMinimal( "OPTION: Clustered execution" );
          }
        }
      }
    }
    if ( configuration.isPassingExport() ) {
      log.logMinimal( "OPTION: Passing export to slave server" );
    }
    log.logMinimal( "OPTION: Logging level : " + configuration.getLogLevel().getDescription() );

    if ( !configuration.getVariables().isEmpty() ) {
      log.logMinimal( "OPTION: Variables: " );
      for ( String variable : configuration.getVariables().keySet() ) {
        log.logMinimal( "  " + variable + " : '" + configuration.getVariables().get( variable ) );
      }
    }
    if ( !configuration.getParams().isEmpty() ) {
      log.logMinimal( "OPTION: Parameters: " );
      for ( String parameter : configuration.getParams().keySet() ) {
        log.logMinimal( "OPTION:   " + parameter + " : '" + configuration.getParams().get( parameter ) );
      }
    }
    if ( configuration.isSafeModeEnabled() ) {
      log.logMinimal( "OPTION: Safe mode enabled" );
    }

    if ( StringUtils.isNotEmpty( queryDelay ) ) {
      log.logMinimal( "OPTION: Remote server query delay : " + getQueryDelay() );
    }
    if ( dontWait ) {
      log.logMinimal( "OPTION: Do not wait for remote job or transformation to finish" );
    }
    if ( remoteLogging ) {
      log.logMinimal( "OPTION: Printing remote execution log" );
    }
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public LogChannel getLog() {
    return log;
  }

  /**
   * Gets metaStore
   *
   * @return value of metaStore
   */
  public IMetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * Gets cmd
   *
   * @return value of cmd
   */
  public CommandLine getCmd() {
    return cmd;
  }

  /**
   * @param cmd The cmd to set
   */
  public void setCmd( CommandLine cmd ) {
    this.cmd = cmd;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * Gets level
   *
   * @return value of level
   */
  public String getLevel() {
    return level;
  }

  /**
   * @param level The level to set
   */
  public void setLevel( String level ) {
    this.level = level;
  }

  /**
   * Gets helpRequested
   *
   * @return value of helpRequested
   */
  public boolean isHelpRequested() {
    return helpRequested;
  }

  /**
   * @param helpRequested The helpRequested to set
   */
  public void setHelpRequested( boolean helpRequested ) {
    this.helpRequested = helpRequested;
  }

  /**
   * Gets parameters
   *
   * @return value of parameters
   */
  public String[] getParameters() {
    return parameters;
  }

  /**
   * @param parameters The parameters to set
   */
  public void setParameters( String[] parameters ) {
    this.parameters = parameters;
  }

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /**
   * @param runConfigurationName The runConfigurationName to set
   */
  public void setRunConfigurationName( String runConfigurationName ) {
    this.runConfigurationName = runConfigurationName;
  }

  /**
   * Gets runTransformation
   *
   * @return value of runTransformation
   */
  public boolean isRunTransformation() {
    return runTransformation;
  }

  /**
   * @param runTransformation The runTransformation to set
   */
  public void setRunTransformation( boolean runTransformation ) {
    this.runTransformation = runTransformation;
  }

  /**
   * Gets runJob
   *
   * @return value of runJob
   */
  public boolean isRunJob() {
    return runJob;
  }

  /**
   * @param runJob The runJob to set
   */
  public void setRunJob( boolean runJob ) {
    this.runJob = runJob;
  }

  /**
   * Gets safeMode
   *
   * @return value of safeMode
   */
  public boolean isSafeMode() {
    return safeMode;
  }

  /**
   * @param safeMode The safeMode to set
   */
  public void setSafeMode( boolean safeMode ) {
    this.safeMode = safeMode;
  }

  /**
   * Gets gatherMetrics
   *
   * @return value of gatherMetrics
   */
  public boolean isGatherMetrics() {
    return gatherMetrics;
  }

  /**
   * @param gatherMetrics The gatherMetrics to set
   */
  public void setGatherMetrics( boolean gatherMetrics ) {
    this.gatherMetrics = gatherMetrics;
  }

  /**
   * Gets slaveServerName
   *
   * @return value of slaveServerName
   */
  public String getSlaveServerName() {
    return slaveServerName;
  }

  /**
   * @param slaveServerName The slaveServerName to set
   */
  public void setSlaveServerName( String slaveServerName ) {
    this.slaveServerName = slaveServerName;
  }

  /**
   * Gets exportToSlaveServer
   *
   * @return value of exportToSlaveServer
   */
  public boolean isExportToSlaveServer() {
    return exportToSlaveServer;
  }

  /**
   * @param exportToSlaveServer The exportToSlaveServer to set
   */
  public void setExportToSlaveServer( boolean exportToSlaveServer ) {
    this.exportToSlaveServer = exportToSlaveServer;
  }

  /**
   * Gets executeClustered
   *
   * @return value of executeClustered
   */
  public boolean isExecuteClustered() {
    return executeClustered;
  }

  /**
   * @param executeClustered The executeClustered to set
   */
  public void setExecuteClustered( boolean executeClustered ) {
    this.executeClustered = executeClustered;
  }

  /**
   * @param queryDelay The queryDelay to set
   */
  public void setQueryDelay( String queryDelay ) {
    this.queryDelay = queryDelay;
  }

  /**
   * Gets dontWait
   *
   * @return value of dontWait
   */
  public boolean isDontWait() {
    return dontWait;
  }

  /**
   * @param dontWait The dontWait to set
   */
  public void setDontWait( boolean dontWait ) {
    this.dontWait = dontWait;
  }

  /**
   * Gets remoteLogging
   *
   * @return value of remoteLogging
   */
  public boolean isRemoteLogging() {
    return remoteLogging;
  }

  /**
   * @param remoteLogging The remoteLogging to set
   */
  public void setRemoteLogging( boolean remoteLogging ) {
    this.remoteLogging = remoteLogging;
  }

  /**
   * Gets printingOptions
   *
   * @return value of printingOptions
   */
  public boolean isPrintingOptions() {
    return printingOptions;
  }

  /**
   * @param printingOptions The printingOptions to set
   */
  public void setPrintingOptions( boolean printingOptions ) {
    this.printingOptions = printingOptions;
  }

  /**
   * Gets intialDir
   *
   * @return value of intialDir
   */
  public String getIntialDir() {
    return intialDir;
  }

  /**
   * @param intialDir The intialDir to set
   */
  public void setIntialDir( String intialDir ) {
    this.intialDir = intialDir;
  }

  /**
   * Gets arguments
   *
   * @return value of arguments
   */
  public String[] getArguments() {
    return arguments;
  }

  /**
   * @param arguments The arguments to set
   */
  public void setArguments( String[] arguments ) {
    this.arguments = arguments;
  }

  public static void main( String[] args ) {

    Maitre maitre = new Maitre();
    try {
      CommandLine cmd = new CommandLine( maitre );
      maitre.setCmd( cmd );
      CommandLine.ParseResult parseResult = cmd.parseArgs( args );
      if ( CommandLine.printHelpIfRequested( parseResult ) ) {
        System.exit( 1 );
      } else {
        maitre.run();
        System.exit( 0 );
      }
    } catch ( ParameterException e ) {
      System.err.println( e.getMessage() );
      e.getCommandLine().usage( System.err );
      System.exit( 9 );
    } catch ( ExecutionException e ) {
      System.err.println( "Error found during execution!" );
      System.err.println( Const.getStackTracker( e ) );

      System.exit( 1 );
    } catch ( Exception e ) {
      System.err.println( "General error found, something went horribly wrong!" );
      System.err.println( Const.getStackTracker( e ) );

      System.exit( 2 );
    }

  }
}
