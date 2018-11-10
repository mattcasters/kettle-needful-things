package org.pentaho.di.needful;

import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElement;
import org.pentaho.metastore.persist.MetaStoreElementType;

@MetaStoreElementType(
  name = "Default Run Configuration",
  description = "A Pentaho Run Configuration as defined in Spoon"
)
public class RunConfiguration {

  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private String server;

  @MetaStoreAttribute
  private boolean clustered;

  @MetaStoreAttribute
  private boolean readOnly;

  @MetaStoreAttribute
  private boolean sendResources;

  @MetaStoreAttribute
  private boolean logRemoteExecutionLocally;

  @MetaStoreAttribute
  private boolean remote;

  @MetaStoreAttribute
  private boolean local;

  @MetaStoreAttribute
  private boolean showTransformations;

  @MetaStoreAttribute
  private String engine;

  public RunConfiguration() {
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets server
   *
   * @return value of server
   */
  public String getServer() {
    return server;
  }

  /**
   * @param server The server to set
   */
  public void setServer( String server ) {
    this.server = server;
  }

  /**
   * Gets clustered
   *
   * @return value of clustered
   */
  public boolean isClustered() {
    return clustered;
  }

  /**
   * @param clustered The clustered to set
   */
  public void setClustered( boolean clustered ) {
    this.clustered = clustered;
  }

  /**
   * Gets readOnly
   *
   * @return value of readOnly
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * @param readOnly The readOnly to set
   */
  public void setReadOnly( boolean readOnly ) {
    this.readOnly = readOnly;
  }

  /**
   * Gets sendResources
   *
   * @return value of sendResources
   */
  public boolean isSendResources() {
    return sendResources;
  }

  /**
   * @param sendResources The sendResources to set
   */
  public void setSendResources( boolean sendResources ) {
    this.sendResources = sendResources;
  }

  /**
   * Gets logRemoteExecutionLocally
   *
   * @return value of logRemoteExecutionLocally
   */
  public boolean isLogRemoteExecutionLocally() {
    return logRemoteExecutionLocally;
  }

  /**
   * @param logRemoteExecutionLocally The logRemoteExecutionLocally to set
   */
  public void setLogRemoteExecutionLocally( boolean logRemoteExecutionLocally ) {
    this.logRemoteExecutionLocally = logRemoteExecutionLocally;
  }

  /**
   * Gets remote
   *
   * @return value of remote
   */
  public boolean isRemote() {
    return remote;
  }

  /**
   * @param remote The remote to set
   */
  public void setRemote( boolean remote ) {
    this.remote = remote;
  }

  /**
   * Gets local
   *
   * @return value of local
   */
  public boolean isLocal() {
    return local;
  }

  /**
   * @param local The local to set
   */
  public void setLocal( boolean local ) {
    this.local = local;
  }

  /**
   * Gets showTransformations
   *
   * @return value of showTransformations
   */
  public boolean isShowTransformations() {
    return showTransformations;
  }

  /**
   * @param showTransformations The showTransformations to set
   */
  public void setShowTransformations( boolean showTransformations ) {
    this.showTransformations = showTransformations;
  }

  /**
   * Gets engine
   *
   * @return value of engine
   */
  public String getEngine() {
    return engine;
  }

  /**
   * @param engine The engine to set
   */
  public void setEngine( String engine ) {
    this.engine = engine;
  }
}
