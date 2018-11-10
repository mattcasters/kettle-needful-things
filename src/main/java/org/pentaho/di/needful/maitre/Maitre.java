package org.pentaho.di.needful.maitre;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.ExecutionConfiguration;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

public class Maitre {
  @Option( names = { "-f", "--file" }, description = "The filename of the job or transformation to run" )
  protected String filename;

  @Option( names = { "-l", "--level" }, description = "The debug level, one of NONE, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL" )
  protected String level;

  @Option( names = { "-h", "--help" }, usageHelp = true, description = "Displays this help message and quits." )
  protected boolean helpRequested = false;

  @Option( names = { "-p", "--parameters" }, description = "A comma separated list of PARAMTER=VALUE pairs", split = "," )
  protected String[] parameters = null;

  @Option( names = { "-r", "-runconfig" }, description = "The name of the Run Configuration to use" )
  protected String runConfiguration = null;

  @Option( names = { "-t", "--transformation" }, description = "Force execution of a transformation" )
  protected boolean runTransformation = false;

  @Option( names = { "-j", "--job" }, description = "Force execution of a job" )
  protected boolean runJob = false;

  @Option( names = { "-a", "--safemode" }, description = "Run in safe mode" )
  protected boolean safeMode = false;

  @Option( names = { "-m", "--metrics" }, description = "Gather metrics" )
  protected boolean gatherMetrics = false;

  @Option( names = { "-s", "--slave" }, description = "The slave server to run on" )
  protected String slaveServerName;

  @Option( names = { "-x", "--export" }, description = "Export all resources and send it to the slave server" )
  protected boolean exportToSlaveServer = false;

  public void execute( CommandLine cmd ) {
    try {
      validateOptions();
      initialize( cmd );
      if ( isTransformation() ) {
        runTransformation( cmd );
      }
      if ( isJob() ) {
        runJob( cmd );
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

  private void runTransformation( CommandLine cmd ) {

    try {
      // Run the transformation with the given filename
      //
      TransMeta transMeta = new TransMeta( filename, true );

      TransExecutionConfiguration configuration = new TransExecutionConfiguration();

      configureBasics(configuration, transMeta.getSharedObjects());

      // Set additional parameters...
      //
      setParametersAndVariables( cmd, configuration, transMeta );



    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of transformation '" + filename + "'", e );
    }
  }

  private void configureBasics( ExecutionConfiguration configuration, SharedObjects sharedObjects ) {

    configuration.setSafeModeEnabled( safeMode );
    configuration.setGatheringMetrics( gatherMetrics );

    if (StringUtils.isNotEmpty( slaveServerName )) {
      SlaveServer slaveServer = (SlaveServer) sharedObjects.getSharedObject( SlaveServer.class.getName(), slaveServerName );
      configuration.setRemoteServer( slaveServer );
    }
    configuration.setPassingExport( exportToSlaveServer );

    configuration.setRunConfiguration( runConfiguration );
  }

  private void runJob( CommandLine cmd ) {
    try {
      // Run the job with the given filename
      //
      JobMeta jobMeta = new JobMeta( filename, null, null );

      JobExecutionConfiguration configuration = new JobExecutionConfiguration();
      configuration.setSafeModeEnabled( safeMode );
      configuration.setGatheringMetrics( gatherMetrics );


      // Set additional parameters...
      //
      setParametersAndVariables( cmd, configuration, jobMeta );



    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of job '" + filename + "'", e );
    }
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

  /**
   * Set the variables and parameters
   *
   * @param cmd
   * @param configuration
   * @param namedParams
   */
  private void setParametersAndVariables( CommandLine cmd, ExecutionConfiguration configuration, NamedParams namedParams ) {
    try {
      String[] availableParameters = namedParams.listParameters();
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
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of transformation '" + filename + "'", e );
    }
  }

  private void validateOptions() {
    if ( StringUtils.isEmpty( filename ) ) {
      throw new ParameterException( new CommandLine( this ), "A filename is needed to run a job or transformation" );
    }
  }

  public static void main( String[] args ) {
    try {
      Maitre maitre = new Maitre();
      CommandLine cmd = new CommandLine( maitre );
      cmd.parseWithHandler( new CommandLine.RunLast().andExit( 2 ), args );
      maitre.execute(cmd);
      System.exit( 0 );
    } catch ( ParameterException e ) {
      System.err.println( e.getMessage() );
      e.getCommandLine().usage( System.err );
      System.exit( 1 );
    }

  }
}
