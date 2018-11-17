package org.pentaho.di.needful.steps.slavestatus;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerStatus;
import org.pentaho.di.www.SlaveServerTransStatus;

public class GetSlaveStatus extends BaseStep implements StepInterface {
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
  public GetSlaveStatus( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                         Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    GetSlaveStatusMeta meta = (GetSlaveStatusMeta) smi;
    GetSlaveStatusData data = (GetSlaveStatusData) sdi;

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
    }

    String slaveName = getInputRowMeta().getString( row, data.slaveIndex );
    SlaveServer slaveServer = getTransMeta().findSlaveServer( slaveName );
    if ( slaveServer == null ) {
      throw new KettleException( "Slave server '" + slaveName + "' can't be found" );
    }

    String errorMessage;
    String statusDescription = null;
    Double serverLoad = null;
    Long memoryFree = null;
    Long memoryTotal = null;
    Long cpuCores = null;
    Long cpuProcessTime = null;
    String osName = null;
    String osVersion = null;
    String osArchitecture = null;
    Long activeTransformations = null;
    Long activeJobs = null;
    Boolean available = null;
    Long responseNs = null;
    long startTime = System.nanoTime();
    long endTime;

    try {
      errorMessage = null;
      SlaveServerStatus status = slaveServer.getStatus();
      serverLoad = status.getLoadAvg();
      memoryFree = Double.valueOf(status.getMemoryFree()).longValue();
      memoryTotal = Double.valueOf(status.getMemoryTotal()).longValue();
      cpuCores = (long) status.getCpuCores();
      cpuProcessTime = status.getCpuProcessTime();
      osName = status.getOsName();
      osVersion = status.getOsVersion();
      osArchitecture = status.getOsArchitecture();
      activeTransformations = 0L;
      for ( SlaveServerTransStatus transStatus : status.getTransStatusList() ) {
        if ( transStatus.isRunning() ) {
          activeTransformations++;
        }
      }
      activeJobs = 0L;
      for ( SlaveServerJobStatus jobStatus : status.getJobStatusList() ) {
        if ( jobStatus.isRunning() ) {
          activeJobs++;
        }
      }

      available = true;
    } catch ( Exception e ) {
      errorMessage = "Error querying slave server : " + e.getMessage();
    } finally {
      endTime = System.nanoTime();
    }
    responseNs = endTime - startTime;

    // Add the fields to the output row
    //
    Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
    int outIndex = getInputRowMeta().size();
    if ( StringUtils.isNotEmpty( meta.getErrorMessageField()) ) {
      outputRow[ outIndex++ ] = errorMessage;
    }
    if ( StringUtils.isNotEmpty( meta.getStatusDescriptionField() ) ) {
      outputRow[ outIndex++ ] = statusDescription;
    }
    if ( StringUtils.isNotEmpty( meta.getServerLoadField() ) ) {
      outputRow[ outIndex++ ] = serverLoad;
    }
    if ( StringUtils.isNotEmpty( meta.getMemoryFreeField() ) ) {
      outputRow[ outIndex++ ] = memoryFree;
    }
    if ( StringUtils.isNotEmpty( meta.getMemoryTotalField() ) ) {
      outputRow[ outIndex++ ] = memoryTotal;
    }
    if ( StringUtils.isNotEmpty( meta.getCpuCoresField() ) ) {
      outputRow[ outIndex++ ] = cpuCores;
    }
    if ( StringUtils.isNotEmpty( meta.getCpuProcessTimeField() ) ) {
      outputRow[ outIndex++ ] = cpuProcessTime;
    }
    if ( StringUtils.isNotEmpty( meta.getOsNameField() ) ) {
      outputRow[ outIndex++ ] = osName;
    }
    if ( StringUtils.isNotEmpty( meta.getOsVersionField() ) ) {
      outputRow[ outIndex++ ] = osVersion;
    }
    if ( StringUtils.isNotEmpty( meta.getOsArchitectureField() ) ) {
      outputRow[ outIndex++ ] = osArchitecture;
    }
    if ( StringUtils.isNotEmpty( meta.getActiveTransformationsField() ) ) {
      outputRow[ outIndex++ ] = activeTransformations;
    }
    if ( StringUtils.isNotEmpty( meta.getActiveJobsField() ) ) {
      outputRow[ outIndex++ ] = activeJobs;
    }
    if ( StringUtils.isNotEmpty( meta.getAvailableField() ) ) {
      outputRow[ outIndex++ ] = available;
    }
    if ( StringUtils.isNotEmpty( meta.getResponseNsField() ) ) {
      outputRow[ outIndex++ ] = responseNs;
    }

    putRow(data.outputRowMeta, outputRow);

    return true;
  }
}
