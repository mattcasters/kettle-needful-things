package org.pentaho.di.needful.metastore;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.trans.Trans;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import java.io.File;

@ExtensionPoint(
  id = "PatchJobMetastoreExtensionPoint",
  extensionPointId = "JobStart",
  description = "Job MetaStore patch: when executing locally through Pan and Kitchen we get no MetaStore reference"
)
public class PatchJobMetastoreExtensionPoint implements ExtensionPointInterface {

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {

    if ( !( object instanceof Job ) ) {
      return; // not for us
    }

    Job job = (Job) object;

    if (job.getJobMeta().getMetaStore()!=null) {
      // Nothing to do here.
      //
      return;
    }

    try {
      DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore();
      IMetaStore localMetaStore = MetaStoreConst.openLocalPentahoMetaStore();
      delegatingMetaStore.addMetaStore( localMetaStore );
      delegatingMetaStore.setActiveMetaStoreName( localMetaStore.getName() );
      job.getJobMeta().setMetaStore( delegatingMetaStore );

      log.logDetailed( "Patched job metastore." );

    } catch(Exception e) {
      log.logError( "Unable to load local MetaStore to patch job", e );
    }
  }
}
