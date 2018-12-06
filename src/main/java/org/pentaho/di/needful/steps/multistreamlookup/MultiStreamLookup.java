package org.pentaho.di.needful.steps.multistreamlookup;

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiStreamLookup extends BaseStep implements StepInterface {

  private boolean lookupValues;
  private MultiStreamLookupMeta meta;
  private MultiStreamLookupData data;

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
  public MultiStreamLookup( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                            Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );

    lookupValues = true;
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (MultiStreamLookupMeta) smi;
    data = (MultiStreamLookupData) sdi;

    if ( lookupValues ) {
      lookupValues = false;
      readLookupValues();
    }

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.inputFieldIndexes = new ArrayList<Integer>();
      for ( int i = 0; i < meta.getLookupActions().size(); i++ ) {
        LookupAction lookupAction = meta.getLookupActions().get( i );

        // At which place is the input field in the input row?
        //
        int inputFieldIndex = data.outputRowMeta.indexOfValue( lookupAction.getInputField() );
        if ( inputFieldIndex < 0 ) {
          throw new KettleException( "Couldn't find input field '" + lookupAction.getInputField() + "'to lookup with " );
        }
        data.inputFieldIndexes.add( inputFieldIndex );
      }
    }

    // Perform the lookups actions
    //
    Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
    int outputFieldIndex = getInputRowMeta().size();
    for ( int actionNr = 0; actionNr < meta.getLookupActions().size(); actionNr++ ) {
      LookupAction lookupAction = meta.getLookupActions().get( actionNr );
      int inputFieldIndex = data.inputFieldIndexes.get( actionNr );
      Object keyValue = data.outputRowMeta.getValueMeta( inputFieldIndex ).convertToNormalStorageType( outputRow[ inputFieldIndex ] );

      // The field indexes for the lookup results
      //
      List<Integer> resultFieldIndexes = data.dataSetResultIndexes.get( actionNr );

      // Which index should we use?
      // We might do lookups on several fields in the data set
      //
      int indexNumber = data.actionFieldToSetIndex.get( actionNr );

      if ( log.isDebug() ) {
        logDebug( "Lookup action #" + ( actionNr + 1 ) + " : " + lookupAction + " with key value " + keyValue+", inputFieldIndex="+inputFieldIndex+", indexNumber="+indexNumber+", resultFieldIndexes="+resultFieldIndexes+", outputRow="+data.outputRowMeta.getString(outputRow) );
      }

      // Pick the index.
      //
      Map<Object, Integer> dataSetIndex = data.dataSetIndexes.get( indexNumber );

      // Which row is it on in the data set?
      //
      Integer dataSetRowNr = dataSetIndex.get( keyValue );
      if ( dataSetRowNr == null ) {
        if (log.isDebug()) {
          logDebug( "Lookup action #" + ( actionNr + 1 ) + " : No results found " );
        }

        // Nothing found...
        // Simply advance the output field index
        //
        outputFieldIndex += lookupAction.getLookupResults().size();
      } else {
        Object[] dataSetRow = data.dataSet.get( dataSetRowNr );

        if (log.isDebug()) {
          logDebug( "Lookup action #" + ( actionNr + 1 ) + " : Found data set row " + dataSetRowNr + " : " + data.dataSetRowMeta.getString( dataSetRow ) );
        }

        // Get the return values...
        //
        List<LookupResult> lookupResults = lookupAction.getLookupResults();
        for ( int resultNr = 0; resultNr < lookupResults.size(); resultNr++ ) {
          LookupResult lookupResult = lookupResults.get( resultNr );

          // Which fields in the data set?
          //
          int resultIndex = resultFieldIndexes.get( resultNr );

          ValueMetaInterface dataSetMeta = data.dataSetRowMeta.getValueMeta( resultIndex );
          Object dataSetValue = dataSetRow[ resultIndex ];
          outputRow[ outputFieldIndex++ ] = dataSetMeta.convertToNormalStorageType( dataSetValue );

          if (log.isDebug()) {
            logDebug( "Add result value at position " + ( outputFieldIndex-1) + " : " + lookupResult+", value : "+ dataSetMeta.getString(dataSetValue));
          }
        }
      }
    }

    putRow( data.outputRowMeta, outputRow );

    return true;
  }

  private void readLookupValues() throws KettleException {

    // Get the output of the previous steps by statically calculating row composition
    //
    List<StepMeta> previousSteps = getTransMeta().findPreviousSteps( getStepMeta(), false );
    if (previousSteps==null || previousSteps.isEmpty()) {
      throw new KettleException( "No previous steps, can't find anything to do" );
    }
    RowMetaInterface prevStepFields = getTransMeta().getStepFields( previousSteps.get( 0 ) );
    data.outputRowMeta = prevStepFields.clone();

    // Now work with the output row metadata, not just the input
    // It will allow lookup values to be used for input again...
    //
    RowMetaInterface[] infoRowMetas = null;
    StepIOMetaInterface stepIOMeta = meta.getStepIOMeta();
    List<StreamInterface> infoStreams = stepIOMeta.getInfoStreams();

    data.infoStream = infoStreams.get( 0 );
    if ( data.infoStream.getStepMeta() == null ) {
      throw new KettleException( "There is no step defined from which to read lookup data from." );
    }

    // Calculate the output row metadata
    //
    if ( infoStreams.size() > 0 ) {
      infoRowMetas = new RowMetaInterface[] { getTransMeta().getStepFields( infoStreams.get( 0 ).getStepname() ) };
    }
    meta.getFields( data.outputRowMeta, getStepname(), infoRowMetas, null, this, repository, metaStore );


    if ( log.isDetailed() ) {
      logDetailed( "Reading lookup data from step : [" + data.infoStream.getStepname() + "]" );
    }

    data.infoRowMeta = getTransMeta().getStepFields( data.infoStream.getStepname() );

    // Which fields do we need to keep around?
    // Which fields do we do lookups on?
    //
    Set<String> dataSetFields = new HashSet<>();
    Set<String> dataSetLookups = new HashSet<>();

    for ( LookupAction lookupAction : meta.getLookupActions() ) {
      dataSetFields.add( lookupAction.getDataSetField() );
      dataSetLookups.add( lookupAction.getDataSetField() );
      for ( LookupResult lookupResult : lookupAction.getLookupResults() ) {
        dataSetFields.add( lookupResult.getDatasetField() );
      }
    }
    data.dataSetFields = new ArrayList( dataSetFields );
    data.dataSetLookups = new ArrayList( dataSetLookups );

    // We need an index per lookup action to know which field index to use.
    //
    data.dataSetRowMeta = new RowMeta();
    data.dataSetFieldIndexes = new ArrayList<Integer>();
    for ( String dataSetField : data.dataSetFields ) {
      int fieldIndex = data.infoRowMeta.indexOfValue( dataSetField );
      if ( fieldIndex < 0 ) {
        throw new KettleException( "Data set field '" + dataSetField + "' couldn't be found reading from step '" + data.infoStream.getStepname() + "'" );
      }
      data.dataSetFieldIndexes.add( fieldIndex );
      data.dataSetRowMeta.addValueMeta( data.infoRowMeta.getValueMeta( fieldIndex ) );
    }
    if (log.isDebug()) {
      logDebug("Data Set Row Meta: "+data.dataSetRowMeta.toString());
    }

    data.dataSetIndexes = new ArrayList<>();
    data.dataSetLookupIndexes = new ArrayList<>();
    for ( String dataSetLookup : data.dataSetLookups ) {
      int lookupIndex = data.dataSetRowMeta.indexOfValue( dataSetLookup );
      if (log.isDetailed()) {
        logDetailed("Lookup field: "+dataSetLookup+", data set row index: "+lookupIndex);
      }
      data.dataSetLookupIndexes.add( lookupIndex );
      data.dataSetIndexes.add( new HashMap<>() );
    }


    // Lookup the result field indexes in the data set rows
    //
    data.actionFieldToSetIndex = new ArrayList<Integer>();
    data.dataSetResultIndexes = new ArrayList<>();
    for ( LookupAction lookupAction : meta.getLookupActions() ) {
      List<Integer> indexes = new ArrayList<>();
      for ( LookupResult lookupResult : lookupAction.getLookupResults() ) {
        int resultFieldIndex = data.dataSetRowMeta.indexOfValue( lookupResult.getDatasetField() );
        indexes.add( resultFieldIndex );
      }
      data.dataSetResultIndexes.add( indexes );

      // Keep track of which lookupAction.getDataSetField() is using which index
      //
      int indexNumber = data.dataSetLookups.indexOf( lookupAction.getDataSetField() );
      data.actionFieldToSetIndex.add( indexNumber );
    }

    // Which row set do we read from?
    //
    data.dataSet = new ArrayList<>();
    int rowNr = 0;
    RowSet rowSet = findInputRowSet( data.infoStream.getStepname() );
    Object[] rowData = getRowFrom( rowSet );
    while ( rowData != null ) {

      // Create a new array to limit memory usage, we only store the used fields...
      // Convert the data to normal storage if it's not.
      //
      Object[] dataSetRow = new Object[data.dataSetFields.size()];
      for (int i=0;i<data.dataSetFields.size();i++) {
        int fieldIndex = data.dataSetFieldIndexes.get(i);
        dataSetRow[i]=data.infoRowMeta.getValueMeta( fieldIndex ).convertToNormalStorageType( rowData[fieldIndex] );
      }
      // Add this data set row to the list...
      //
      data.dataSet.add( dataSetRow );

      // Update the lookup indexes...
      //
      for ( int i = 0; i < data.dataSetLookups.size(); i++ ) {
        String dataSetLookup = data.dataSetLookups.get( i );
        int lookupIndex = data.dataSetLookupIndexes.get( i );
        Object key = data.dataSetRowMeta.getValueMeta( lookupIndex ).convertToNormalStorageType( dataSetRow[ lookupIndex ] );

        Map<Object, Integer> rowNrMap = data.dataSetIndexes.get( i );
        rowNrMap.put( key, rowNr );
      }

      rowNr++;
      rowData = getRowFrom( rowSet );
    }
  }
}
