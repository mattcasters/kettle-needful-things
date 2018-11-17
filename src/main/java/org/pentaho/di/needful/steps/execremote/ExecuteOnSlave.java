package org.pentaho.di.needful.steps.execremote;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.ExecutionConfiguration;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.needful.maitre.Maitre;
import org.pentaho.di.needful.shared.ParameterDetails;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.List;

public class ExecuteOnSlave extends BaseStep implements StepInterface {
  /**
   * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
   * steps.
   *
   * @param stepMeta          The StepMeta object to run.
   * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
   *                          hashtables etc.
   * @param copyNr            The copynumber for this step.
   * @param transMeta         The TransInfo of which the step stepMeta is part of.
   * @param trans             The (running) transformation to obtain information shared among the steps.
   */
  public ExecuteOnSlave( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                         Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    ExecuteOnSlaveMeta meta = (ExecuteOnSlaveMeta) smi;
    ExecuteOnSlaveData data = (ExecuteOnSlaveData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      // Get the row layout
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

      data.slaveIndex = getInputRowMeta().indexOfValue( meta.getSlaveField() );
      if ( data.slaveIndex < 0 ) {
        throw new KettleException( "Unable to find slave server field '" + meta.getSlaveField() );
      }

      data.filenameIndex = getInputRowMeta().indexOfValue( meta.getFilenameField() );
      if ( data.filenameIndex < 0 ) {
        throw new KettleException( "Unable to find (.ktr/.kjb) file name field '" + meta.getFilenameField() );
      }
    }

    String slaveName = getInputRowMeta().getString( row, data.slaveIndex );
    SlaveServer slaveServer = getTransMeta().findSlaveServer( slaveName );
    if ( slaveServer == null ) {
      throw new KettleException( "Slave server '" + slaveName + "' can't be found" );
    }
    slaveServer.shareVariablesWith( this );

    String filenameInput = getInputRowMeta().getString( row, data.filenameIndex );
    if ( StringUtils.isEmpty( filenameInput ) ) {
      throw new KettleException( "The file name field '" + meta.getFilenameField() + "' is empty" );
    }

    // Get the correct filename
    //
    String filename = environmentSubstitute( filenameInput );

    Result result = null;

    // Transformation
    //
    if ( filename.toLowerCase().endsWith( ".ktr" ) ) {

      TransMeta transMeta = new TransMeta( filename, metaStore, repository, true, getTrans(), null );

      TransExecutionConfiguration configuration = new TransExecutionConfiguration();
      parseParametersAndVariables( meta.getParameters(), getInputRowMeta(), row, configuration, transMeta );

      configureExecution( meta, slaveServer, configuration );
      configuration.setLogRemoteExecutionLocally( true );

      try {
        result = Maitre.runTransformationOnSlaveServer( log, transMeta, slaveServer, configuration, metaStore, meta.isNotWaiting(), 10 );
      } catch ( Exception e ) {
        throw new KettleException( "Error running transformation on slave server: " + e.getMessage(), e );
      }
    } else {
      JobMeta jobMeta = new JobMeta( this, filename, repository, metaStore, null );

      JobExecutionConfiguration configuration = new JobExecutionConfiguration();
      parseParametersAndVariables( meta.getParameters(), getInputRowMeta(), row, configuration, jobMeta );

      configureExecution( meta, slaveServer, configuration );

      try {
        result = Maitre.runJobOnSlaveServer( log, jobMeta, slaveServer, configuration, metaStore, meta.isNotWaiting(), meta.isExportingResources(), 10 );
      } catch(Exception e) {
        throw new KettleException( "Error running job on slave server: " + e.getMessage(), e );
      }
    }

    // Add the fields to the output row
    //
    Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
    int outIndex = getInputRowMeta().size();
    if ( result!=null ) {
      outputRow[ outIndex++ ] = result.getXML();
    }

    putRow( data.outputRowMeta, outputRow );

    return true;
  }

  private void configureExecution( ExecuteOnSlaveMeta meta, SlaveServer slaveServer, ExecutionConfiguration configuration ) {
    configuration.setRemoteServer( slaveServer );
    configuration.setLogLevel( getLogLevel() );
    configuration.setPassingExport( meta.isExportingResources() );
    configuration.setRepository( repository );
  }


  /**
   * Set the variables and parameters
   *
   * @param parameters
   * @param rowMeta
   * @param rowData
   * @param configuration
   * @param namedParams
   * @throws KettleException
   */
  private void parseParametersAndVariables( List<ParameterDetails> parameters, RowMetaInterface rowMeta, Object[] rowData, ExecutionConfiguration configuration, NamedParams namedParams )
    throws KettleException {
    try {
      String[] availableParameters = namedParams.listParameters();
      for ( ParameterDetails parameter : parameters ) {
        String key = parameter.getName();
        String field = parameter.getField();
        String value = StringUtils.isNotEmpty( field ) ? rowMeta.getString( rowData, field, null ) : null;
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
      throw new KettleException( "There was an error setting variables and parameters", e );
    }
  }
}
