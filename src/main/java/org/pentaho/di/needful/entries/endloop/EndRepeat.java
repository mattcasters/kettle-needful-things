package org.pentaho.di.needful.entries.endloop;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.needful.entries.repeat.Repeat;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@JobEntry(
  id = "EndRepeat",
  name = "End Repeat",
  description = "End repeated execution of a job or a transformation",
  categoryDescription = "General",
  image = "ui/images/SUC.svg"
)
public class EndRepeat extends JobEntryBase implements JobEntryInterface, Cloneable {

  public EndRepeat( String name, String description) {
    super( name, description );
  }

  public EndRepeat() {
    this( "", "" );
  }

  /**
   *  Simply set a flag in the parent job, this is also a success
   *
   * @param prevResult
   * @param nr
   * @return
   * @throws KettleException
   */
  @Override public Result execute( Result prevResult, int nr ) throws KettleException {

    parentJob.getExtensionDataMap().put( Repeat.REPEAT_END_LOOP, getName() );

    // Force success.
    //
    prevResult.setResult( true );
    prevResult.setNrErrors( 0 );

    return prevResult;
  }

  @Override public EndRepeat clone() {
    return (EndRepeat) super.clone();
  }

  @Override public String getDialogClassName() {
    return EndRepeatDialog.class.getName();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( super.getXML() );

    return retval.toString();
  }

  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
                       Repository rep, IMetaStore metaStore ) throws KettleXMLException {
    try {
      super.loadXML( entrynode, databases, slaveServers );
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load End Repeat job entry metadata from XML", e );
    }
  }

  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
  }

  // Save the attributes of this job entry
  //
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
  }

}
