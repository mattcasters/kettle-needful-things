package org.pentaho.di.needful.steps.execremote;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class ExecuteOnSlaveData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public int slaveIndex;
  public int filenameIndex;
}
