package org.pentaho.di.needful.metastore;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.www.SlaveServerConfig;
import org.pentaho.di.www.WebServer;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

import java.io.File;

@ExtensionPoint(
  id = "CarteStartupPatchMetastoreExtensionPoint",
  extensionPointId = "CarteStartup",
  description = "Patch local metastore location (bug fix)"
)
public class CarteStartupPatchMetastoreExtensionPoint implements ExtensionPointInterface {

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {

    if ( !( object instanceof WebServer ) ) {
      return; // not for us
    }

    WebServer webServer = (WebServer) object;

    SlaveServerConfig slaveServerConfig = webServer.getTransformationMap().getSlaveServerConfig();
    DelegatingMetaStore delegatingMetaStore = slaveServerConfig.getMetaStore();

    String defaultLocation = MetaStoreConst.getDefaultPentahoMetaStoreLocation()+ File.separator + "metastore";

    // Find the local metastore
    //
    int index = -1;
    for ( int i = 0; i < delegatingMetaStore.getMetaStoreList().size(); i++ ) {
      IMetaStore metaStore = delegatingMetaStore.getMetaStoreList().get( i );

      try {
        if (metaStore instanceof XmlMetaStore ) {
          XmlMetaStore xmlMetaStore = (XmlMetaStore) metaStore;

          if ( xmlMetaStore.getRootFolder().equals(defaultLocation )
          ) {
            // Replace this one with the correct metastore taking into account the PENTAHO_METASTORE_FOLDER variable
            //
            index = i;
          }
        }
      } catch ( Exception e ) {
        // Log, ignore otherwise
        //
        log.logError( "Unable to get name from metastore " + metaStore, e );
      }
    }

    try {
      IMetaStore correctMetaStore = MetaStoreConst.openLocalPentahoMetaStore();
      if ( index >= 0 ) {
        delegatingMetaStore.getMetaStoreList().set( index, correctMetaStore );
        log.logBasic("Needful things replaced the local metastore.");

      } else {
        // Not found?  Add it at the beginning (bottom of hierarchy)
        //
        delegatingMetaStore.getMetaStoreList().add( 0, correctMetaStore );
        log.logBasic("Needful things add the local metastore at index 0.");
      }
      delegatingMetaStore.setActiveMetaStoreName( correctMetaStore.getName() );

      log.logBasic(">>>>>>>>>>>>>>>>>>>>> DELEGATE METASTORE CONTENTS <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
      for (IMetaStore metaStore : delegatingMetaStore.getMetaStoreList()) {
        log.logBasic("Metastore found : "+metaStore.getName());
      }

      log.logBasic(">>>>>>>>>>>>>>>>>>>>> METASTORE CONTENTS <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
      for (String namespace :  delegatingMetaStore.getNamespaces()) {
        log.logBasic("Found namespace : "+namespace);
        for ( IMetaStoreElementType elementType : delegatingMetaStore.getElementTypes( namespace )) {
          log.logBasic("  Found type : "+elementType.getName()+" : "+delegatingMetaStore.getElements(namespace, elementType).size()+" elements found");
        }
      }
      log.logBasic("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

    } catch ( MetaStoreException e ) {
      throw new KettleException( "Unable to load metastore in needful things plugin (patch metastore)", e );
    }
  }
}
