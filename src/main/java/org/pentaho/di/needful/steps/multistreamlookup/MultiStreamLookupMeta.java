package org.pentaho.di.needful.steps.multistreamlookup;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepIOMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

@Step(
  id = "MultiStreamLookup",
  name = "Multi Stream Lookup",
  description = "Lookup multiple values in a single data set loaded in memory",
  image = "ui/images/SLU.svg",
  categoryDescription = "Lookup"
)
public class MultiStreamLookupMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String TAG_LOOKUP_ACTIONS = "lookup_actions";
  public static final String TAG_LOOKUP_ACTION = "lookup_action";
  public static final String TAG_INPUT_FIELD = "input_field";
  public static final String TAG_LOOKUP_RESULTS = "lookup_results";
  public static final String TAG_LOOKUP_RESULT = "lookup_result";
  public static final String TAG_DATASET_FIELD = "dataset_field";
  public static final String TAG_RENAME_TO = "rename_to";
  public static final String TAG_INFO_STEP_NAME = "info_step_name";

  private List<LookupAction> lookupActions;

  public MultiStreamLookupMeta() {
    lookupActions = new ArrayList<>();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {
    if ( info != null && info.length == 1 && info[ 0 ] != null ) {
      for ( int i = 0; i < lookupActions.size(); i++ ) {
        LookupAction lookupAction = lookupActions.get( i );

        // Every lookup action can have multiple output fields...
        //
        for ( LookupResult lookupResult : lookupAction.getLookupResults() ) {

          ValueMetaInterface v = info[ 0 ].searchValueMeta( lookupResult.getDatasetField() );
          if ( v != null ) {
            ValueMetaInterface returnValue = v.clone();
            if ( StringUtils.isNotEmpty( lookupResult.getRenameTo() ) ) {
              returnValue.setName( lookupResult.getRenameTo() );
            }
            returnValue.setOrigin( name );
            inputRowMeta.addValueMeta( returnValue );
          } else {
            throw new KettleStepException( "Unable to find data set field '" + lookupResult.getDatasetField() + "', lookup with input field '" + lookupAction.getInputField() + "'" );
          }
        }
      }
    }
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new MultiStreamLookup( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new MultiStreamLookupData();
  }

  @Override public String getDialogClassName() {
    return MultiStreamLookupDialog.class.getName();
  }

  @Override public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();

    // Log the source info step...
    //
    StepIOMetaInterface ioMeta = getStepIOMeta();
    List<StreamInterface> infoStreams = ioMeta.getInfoStreams();
    StreamInterface infoStream = infoStreams.get( 0 );
    xml.append( XMLHandler.addTagValue( TAG_INFO_STEP_NAME, infoStream.getStepname() ) );

    xml.append( XMLHandler.openTag( TAG_LOOKUP_ACTIONS ) );
    for ( LookupAction lookupAction : lookupActions ) {
      xml.append( XMLHandler.openTag( TAG_LOOKUP_ACTION ) );

      xml.append( XMLHandler.addTagValue( TAG_INPUT_FIELD, lookupAction.getInputField() ) );
      xml.append( XMLHandler.addTagValue( TAG_DATASET_FIELD, lookupAction.getDataSetField() ) );

      xml.append( XMLHandler.openTag( TAG_LOOKUP_RESULTS ) );
      for ( LookupResult lookupResult : lookupAction.getLookupResults() ) {
        xml.append( XMLHandler.openTag( TAG_LOOKUP_RESULT ) );

        xml.append( XMLHandler.addTagValue( TAG_INPUT_FIELD, lookupResult.getDatasetField() ) );
        xml.append( XMLHandler.addTagValue( TAG_RENAME_TO, lookupResult.getRenameTo() ) );

        xml.append( XMLHandler.closeTag( TAG_LOOKUP_RESULT ) );
      }
      xml.append( XMLHandler.closeTag( TAG_LOOKUP_RESULTS ) );

      xml.append( XMLHandler.closeTag( TAG_LOOKUP_ACTION ) );
    }
    xml.append( XMLHandler.closeTag( TAG_LOOKUP_ACTIONS ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {


    // Handle step source information...
    //
    String lookupFromStepname = XMLHandler.getTagValue( stepnode, TAG_INFO_STEP_NAME );
    StreamInterface infoStream = getStepIOMeta().getInfoStreams().get( 0 );
    infoStream.setSubject( lookupFromStepname );

    // The rest of the metadata...

    Node lookupActionsNode = XMLHandler.getSubNode( stepnode, TAG_LOOKUP_ACTIONS );
    List<Node> lookupActionNodes = XMLHandler.getNodes( lookupActionsNode, TAG_LOOKUP_ACTION );
    lookupActions = new ArrayList<>();
    for ( Node lookupActionNode : lookupActionNodes ) {
      String inputField = XMLHandler.getTagValue( lookupActionNode, TAG_INPUT_FIELD );
      String dataSetField = XMLHandler.getTagValue( lookupActionNode, TAG_DATASET_FIELD );
      List<LookupResult> lookupResults = new ArrayList<>();
      Node lookupResultsNode = XMLHandler.getSubNode( lookupActionNode, TAG_LOOKUP_RESULTS );
      List<Node> lookupResultNodes = XMLHandler.getNodes( lookupResultsNode, TAG_LOOKUP_RESULT );
      for ( Node lookupResultNode : lookupResultNodes ) {
        String dataSetReturnField = XMLHandler.getTagValue( lookupResultNode, TAG_INPUT_FIELD );
        String renameTo = XMLHandler.getTagValue( lookupResultNode, TAG_RENAME_TO );
        lookupResults.add( new LookupResult( dataSetReturnField, renameTo ) );
      }
      lookupActions.add( new LookupAction( inputField, dataSetField, lookupResults ) );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    throw new KettleException( "Saving to a repository is not yet implemented" );
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    throw new KettleException( "Loading step metadata from a repository is not yet implemented" );
  }

  @Override public void setDefault() {
    lookupActions = new ArrayList<>();
  }

  @Override public StepIOMetaInterface getStepIOMeta() {

    // Get ioMeta in a compatible way.
    // Figure out if there's an ioMeta field.
    // In pre-8.2 there is, in 8.2 there isn't.
    //
    StepIOMetaInterface io;
    try {
      Field ioMetaField = MultiStreamLookupMeta.class.getDeclaredField( "ioMeta" );
      io = (StepIOMetaInterface) ioMetaField.get(this);

    } catch( NoSuchFieldException | IllegalAccessException fe) {
      // Don't create it, we'll do this!
      io = super.getStepIOMeta(false);
    }

    if ( io == null ) {

      io = new StepIOMeta( true, true, false, false, false, false );

      StreamInterface stream = new Stream(
        StreamInterface.StreamType.INFO,
        null,
        "These rows are loaded into memory and used to perform lookups on.", StreamIcon.INFO, null );
      io.addStream( stream );
      try {
        setStepIOMeta(io);
      } catch(java.lang.NoSuchMethodError e) {
        /**
         * This is a pre-8.2 API, setStepIOMeta() is not available
         * However, ioMeta is not available either so we change it using the API
         */
        try {
          Field ioMetaField = MultiStreamLookupMeta.class.getDeclaredField( "ioMeta" );
          // ioMetaField.setAccessible( true );
          ioMetaField.set( this, io );
        } catch(Exception fe) {
          throw new RuntimeException( "Unable to find or set the Step IO Metadata",e );
        }
      }
    }

    return io;
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    for ( StreamInterface stream : getStepIOMeta().getInfoStreams() ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  @Override
  public void resetStepIoMeta() {
    // Do nothing, don't reset as there is no need to do this.
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }


  @Override public MultiStreamLookupMeta clone() {

    MultiStreamLookupMeta meta = new MultiStreamLookupMeta();
    for (LookupAction lookupAction : lookupActions) {
      meta.getLookupActions().add(new LookupAction(lookupAction));
    }
    return meta;
  }

  /**
   * Gets lookupActions
   *
   * @return value of lookupActions
   */
  public List<LookupAction> getLookupActions() {
    return lookupActions;
  }

  /**
   * @param lookupActions The lookupActions to set
   */
  public void setLookupActions( List<LookupAction> lookupActions ) {
    this.lookupActions = lookupActions;
  }
}
