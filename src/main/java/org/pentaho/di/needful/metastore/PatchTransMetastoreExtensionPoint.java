package org.pentaho.di.needful.metastore;

import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.SlaveServerConfig;
import org.pentaho.di.www.WebServer;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

import java.io.File;

@ExtensionPoint(
  id = "PatchTransMetastoreExtensionPoint",
  extensionPointId = "TransformationPrepareExecution",
  description = "Trans MetaStore patch : When executing locally through Pan and Kitchen we get no MetaStore reference"
)
public class PatchTransMetastoreExtensionPoint implements ExtensionPointInterface {

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {

    if ( !( object instanceof Trans ) ) {
      return; // not for us
    }

    Trans trans = (Trans) object;

    if (trans.getMetaStore()!=null) {
      // Nothing to do here.
      //
      return;
    }

    try {
      DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore();
      IMetaStore localMetaStore = MetaStoreConst.openLocalPentahoMetaStore();
      delegatingMetaStore.addMetaStore( localMetaStore );
      delegatingMetaStore.setActiveMetaStoreName( localMetaStore.getName() );
      trans.setMetaStore( delegatingMetaStore );

      if (trans.getTransMeta().getMetaStore()==null) {
        trans.getTransMeta().setMetaStore( delegatingMetaStore );
      }
      log.logDetailed( "Patched transformation MetaStore." );

    } catch(Exception e) {
      log.logError( "Unable to load local MetaStore to patch transformation", e );
    }
  }
}
