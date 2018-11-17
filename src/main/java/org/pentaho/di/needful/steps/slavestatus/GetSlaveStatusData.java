package org.pentaho.di.needful.steps.slavestatus;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class GetSlaveStatusData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public int slaveIndex;
}
