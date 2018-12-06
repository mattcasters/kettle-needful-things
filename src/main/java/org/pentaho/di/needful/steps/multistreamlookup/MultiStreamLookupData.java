package org.pentaho.di.needful.steps.multistreamlookup;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiStreamLookupData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public StreamInterface infoStream;
  public RowMetaInterface infoRowMeta;
  public Map<Object, Object[]> dataMap;
  public List<Object[]> dataSet;
  public RowMetaInterface dataSetRowMeta;
  public List<Integer> dataSetFieldIndexes;
  public List<Map<Object, Integer>> dataSetIndexes;
  public List<String> dataSetFields;
  public List<String> dataSetLookups;
  public List<Integer> dataSetLookupIndexes;
  public List<Integer> inputFieldIndexes;

  public List<List<Integer>> dataSetResultIndexes;
  public List<Integer> actionFieldToSetIndex;
}
