
package org.pentaho.di.needful.steps.slavestatus;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class GetSlaveStatusDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = GetSlaveStatus.class; // for i18n purposes, needed by Translator2!!
  private final GetSlaveStatusMeta input;

  int middle;
  int margin;

  private boolean getpreviousFields = false;

  private Combo wSlaveField;
  private Text wErrorMessage;
  private Text wStatusDescription;
  private Text wServerLoad;
  private Text wMemoryFree;
  private Text wMemoryTotal;
  private Text wCpuCores;
  private Text wCpuProcessTime;
  private Text wOsName;
  private Text wOsVersion;
  private Text wOsArchitecture;
  private Text wActiveTransformations;
  private Text wActiveJobs;
  private Text wAvailable;
  private Text wResponseNs;

  public GetSlaveStatusDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (GetSlaveStatusMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    Label wlSlaveField = new Label( shell, SWT.RIGHT );
    wlSlaveField.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.SlaveField" ) );
    props.setLook( wlSlaveField );
    FormData fdlSlaveField = new FormData();
    fdlSlaveField.left = new FormAttachment( 0, 0 );
    fdlSlaveField.top = new FormAttachment( lastControl, margin );
    fdlSlaveField.right = new FormAttachment( middle, -margin );
    wlSlaveField.setLayoutData( fdlSlaveField );
    wSlaveField = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSlaveField.setText( stepname );
    props.setLook( wSlaveField );
    FormData fdSlaveField = new FormData();
    fdSlaveField.left = new FormAttachment( middle, 0 );
    fdSlaveField.top = new FormAttachment( wlSlaveField, 0, SWT.CENTER );
    fdSlaveField.right = new FormAttachment( 100, 0 );
    wSlaveField.setLayoutData( fdSlaveField );
    lastControl = wSlaveField;

    Label wlErrorMessage = new Label( shell, SWT.RIGHT );
    wlErrorMessage.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.ErrorMessage" ) );
    props.setLook( wlErrorMessage );
    FormData fdlErrorMessage = new FormData();
    fdlErrorMessage.left = new FormAttachment( 0, 0 );
    fdlErrorMessage.top = new FormAttachment( lastControl, margin );
    fdlErrorMessage.right = new FormAttachment( middle, -margin );
    wlErrorMessage.setLayoutData( fdlErrorMessage );
    wErrorMessage = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wErrorMessage.setText( stepname );
    props.setLook( wErrorMessage );
    FormData fdErrorMessage = new FormData();
    fdErrorMessage.left = new FormAttachment( middle, 0 );
    fdErrorMessage.top = new FormAttachment( wlErrorMessage, 0, SWT.CENTER );
    fdErrorMessage.right = new FormAttachment( 100, 0 );
    wErrorMessage.setLayoutData( fdErrorMessage );
    lastControl = wErrorMessage;

    Label wlStatusDescription = new Label( shell, SWT.RIGHT );
    wlStatusDescription.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.StatusDescription" ) );
    props.setLook( wlStatusDescription );
    FormData fdlStatusDescription = new FormData();
    fdlStatusDescription.left = new FormAttachment( 0, 0 );
    fdlStatusDescription.top = new FormAttachment( lastControl, margin );
    fdlStatusDescription.right = new FormAttachment( middle, -margin );
    wlStatusDescription.setLayoutData( fdlStatusDescription );
    wStatusDescription = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStatusDescription.setText( stepname );
    props.setLook( wStatusDescription );
    FormData fdStatusDescription = new FormData();
    fdStatusDescription.left = new FormAttachment( middle, 0 );
    fdStatusDescription.top = new FormAttachment( wlStatusDescription, 0, SWT.CENTER );
    fdStatusDescription.right = new FormAttachment( 100, 0 );
    wStatusDescription.setLayoutData( fdStatusDescription );
    lastControl = wStatusDescription;

    Label wlServerLoad = new Label( shell, SWT.RIGHT );
    wlServerLoad.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.ServerLoad" ) );
    props.setLook( wlServerLoad );
    FormData fdlServerLoad = new FormData();
    fdlServerLoad.left = new FormAttachment( 0, 0 );
    fdlServerLoad.top = new FormAttachment( lastControl, margin );
    fdlServerLoad.right = new FormAttachment( middle, -margin );
    wlServerLoad.setLayoutData( fdlServerLoad );
    wServerLoad = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wServerLoad.setText( stepname );
    props.setLook( wServerLoad );
    FormData fdServerLoad = new FormData();
    fdServerLoad.left = new FormAttachment( middle, 0 );
    fdServerLoad.top = new FormAttachment( wlServerLoad, 0, SWT.CENTER );
    fdServerLoad.right = new FormAttachment( 100, 0 );
    wServerLoad.setLayoutData( fdServerLoad );
    lastControl = wServerLoad;

    Label wlMemoryFree = new Label( shell, SWT.RIGHT );
    wlMemoryFree.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.MemoryFree" ) );
    props.setLook( wlMemoryFree );
    FormData fdlMemoryFree = new FormData();
    fdlMemoryFree.left = new FormAttachment( 0, 0 );
    fdlMemoryFree.top = new FormAttachment( lastControl, margin );
    fdlMemoryFree.right = new FormAttachment( middle, -margin );
    wlMemoryFree.setLayoutData( fdlMemoryFree );
    wMemoryFree = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wMemoryFree.setText( stepname );
    props.setLook( wMemoryFree );
    FormData fdMemoryFree = new FormData();
    fdMemoryFree.left = new FormAttachment( middle, 0 );
    fdMemoryFree.top = new FormAttachment( wlMemoryFree, 0, SWT.CENTER );
    fdMemoryFree.right = new FormAttachment( 100, 0 );
    wMemoryFree.setLayoutData( fdMemoryFree );
    lastControl = wMemoryFree;

    Label wlMemoryTotal = new Label( shell, SWT.RIGHT );
    wlMemoryTotal.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.MemoryTotal" ) );
    props.setLook( wlMemoryTotal );
    FormData fdlMemoryTotal = new FormData();
    fdlMemoryTotal.left = new FormAttachment( 0, 0 );
    fdlMemoryTotal.top = new FormAttachment( lastControl, margin );
    fdlMemoryTotal.right = new FormAttachment( middle, -margin );
    wlMemoryTotal.setLayoutData( fdlMemoryTotal );
    wMemoryTotal = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wMemoryTotal.setText( stepname );
    props.setLook( wMemoryTotal );
    FormData fdMemoryTotal = new FormData();
    fdMemoryTotal.left = new FormAttachment( middle, 0 );
    fdMemoryTotal.top = new FormAttachment( wlMemoryTotal, 0, SWT.CENTER );
    fdMemoryTotal.right = new FormAttachment( 100, 0 );
    wMemoryTotal.setLayoutData( fdMemoryTotal );
    lastControl = wMemoryTotal;

    Label wlCpuCores = new Label( shell, SWT.RIGHT );
    wlCpuCores.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.CpuCores" ) );
    props.setLook( wlCpuCores );
    FormData fdlCpuCores = new FormData();
    fdlCpuCores.left = new FormAttachment( 0, 0 );
    fdlCpuCores.top = new FormAttachment( lastControl, margin );
    fdlCpuCores.right = new FormAttachment( middle, -margin );
    wlCpuCores.setLayoutData( fdlCpuCores );
    wCpuCores = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wCpuCores.setText( stepname );
    props.setLook( wCpuCores );
    FormData fdCpuCores = new FormData();
    fdCpuCores.left = new FormAttachment( middle, 0 );
    fdCpuCores.top = new FormAttachment( wlCpuCores, 0, SWT.CENTER );
    fdCpuCores.right = new FormAttachment( 100, 0 );
    wCpuCores.setLayoutData( fdCpuCores );
    lastControl = wCpuCores;

    Label wlCpuProcessTime = new Label( shell, SWT.RIGHT );
    wlCpuProcessTime.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.CpuProcessTime" ) );
    props.setLook( wlCpuProcessTime );
    FormData fdlCpuProcessTime = new FormData();
    fdlCpuProcessTime.left = new FormAttachment( 0, 0 );
    fdlCpuProcessTime.top = new FormAttachment( lastControl, margin );
    fdlCpuProcessTime.right = new FormAttachment( middle, -margin );
    wlCpuProcessTime.setLayoutData( fdlCpuProcessTime );
    wCpuProcessTime = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wCpuProcessTime.setText( stepname );
    props.setLook( wCpuProcessTime );
    FormData fdCpuProcessTime = new FormData();
    fdCpuProcessTime.left = new FormAttachment( middle, 0 );
    fdCpuProcessTime.top = new FormAttachment( wlCpuProcessTime, 0, SWT.CENTER );
    fdCpuProcessTime.right = new FormAttachment( 100, 0 );
    wCpuProcessTime.setLayoutData( fdCpuProcessTime );
    lastControl = wCpuProcessTime;

    Label wlOsName = new Label( shell, SWT.RIGHT );
    wlOsName.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.OsName" ) );
    props.setLook( wlOsName );
    FormData fdlOsName = new FormData();
    fdlOsName.left = new FormAttachment( 0, 0 );
    fdlOsName.top = new FormAttachment( lastControl, margin );
    fdlOsName.right = new FormAttachment( middle, -margin );
    wlOsName.setLayoutData( fdlOsName );
    wOsName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOsName.setText( stepname );
    props.setLook( wOsName );
    FormData fdOsName = new FormData();
    fdOsName.left = new FormAttachment( middle, 0 );
    fdOsName.top = new FormAttachment( wlOsName, 0, SWT.CENTER );
    fdOsName.right = new FormAttachment( 100, 0 );
    wOsName.setLayoutData( fdOsName );
    lastControl = wOsName;

    Label wlOsVersion = new Label( shell, SWT.RIGHT );
    wlOsVersion.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.OsVersion" ) );
    props.setLook( wlOsVersion );
    FormData fdlOsVersion = new FormData();
    fdlOsVersion.left = new FormAttachment( 0, 0 );
    fdlOsVersion.top = new FormAttachment( lastControl, margin );
    fdlOsVersion.right = new FormAttachment( middle, -margin );
    wlOsVersion.setLayoutData( fdlOsVersion );
    wOsVersion = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOsVersion.setText( stepname );
    props.setLook( wOsVersion );
    FormData fdOsVersion = new FormData();
    fdOsVersion.left = new FormAttachment( middle, 0 );
    fdOsVersion.top = new FormAttachment( wlOsVersion, 0, SWT.CENTER );
    fdOsVersion.right = new FormAttachment( 100, 0 );
    wOsVersion.setLayoutData( fdOsVersion );
    lastControl = wOsVersion;

    Label wlOsArchitecture = new Label( shell, SWT.RIGHT );
    wlOsArchitecture.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.OsArchitecture" ) );
    props.setLook( wlOsArchitecture );
    FormData fdlOsArchitecture = new FormData();
    fdlOsArchitecture.left = new FormAttachment( 0, 0 );
    fdlOsArchitecture.top = new FormAttachment( lastControl, margin );
    fdlOsArchitecture.right = new FormAttachment( middle, -margin );
    wlOsArchitecture.setLayoutData( fdlOsArchitecture );
    wOsArchitecture = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOsArchitecture.setText( stepname );
    props.setLook( wOsArchitecture );
    FormData fdOsArchitecture = new FormData();
    fdOsArchitecture.left = new FormAttachment( middle, 0 );
    fdOsArchitecture.top = new FormAttachment( wlOsArchitecture, 0, SWT.CENTER );
    fdOsArchitecture.right = new FormAttachment( 100, 0 );
    wOsArchitecture.setLayoutData( fdOsArchitecture );
    lastControl = wOsArchitecture;

    Label wlActiveTransformations = new Label( shell, SWT.RIGHT );
    wlActiveTransformations.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.ActiveTransformations" ) );
    props.setLook( wlActiveTransformations );
    FormData fdlActiveTransformations = new FormData();
    fdlActiveTransformations.left = new FormAttachment( 0, 0 );
    fdlActiveTransformations.top = new FormAttachment( lastControl, margin );
    fdlActiveTransformations.right = new FormAttachment( middle, -margin );
    wlActiveTransformations.setLayoutData( fdlActiveTransformations );
    wActiveTransformations = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wActiveTransformations.setText( stepname );
    props.setLook( wActiveTransformations );
    FormData fdActiveTransformations = new FormData();
    fdActiveTransformations.left = new FormAttachment( middle, 0 );
    fdActiveTransformations.top = new FormAttachment( wlActiveTransformations, 0, SWT.CENTER );
    fdActiveTransformations.right = new FormAttachment( 100, 0 );
    wActiveTransformations.setLayoutData( fdActiveTransformations );
    lastControl = wActiveTransformations;

    Label wlActiveJobs = new Label( shell, SWT.RIGHT );
    wlActiveJobs.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.ActiveJobs" ) );
    props.setLook( wlActiveJobs );
    FormData fdlActiveJobs = new FormData();
    fdlActiveJobs.left = new FormAttachment( 0, 0 );
    fdlActiveJobs.top = new FormAttachment( lastControl, margin );
    fdlActiveJobs.right = new FormAttachment( middle, -margin );
    wlActiveJobs.setLayoutData( fdlActiveJobs );
    wActiveJobs = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wActiveJobs.setText( stepname );
    props.setLook( wActiveJobs );
    FormData fdActiveJobs = new FormData();
    fdActiveJobs.left = new FormAttachment( middle, 0 );
    fdActiveJobs.top = new FormAttachment( wlActiveJobs, 0, SWT.CENTER );
    fdActiveJobs.right = new FormAttachment( 100, 0 );
    wActiveJobs.setLayoutData( fdActiveJobs );
    lastControl = wActiveJobs;

    Label wlAvailable = new Label( shell, SWT.RIGHT );
    wlAvailable.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.Available" ) );
    props.setLook( wlAvailable );
    FormData fdlAvailable = new FormData();
    fdlAvailable.left = new FormAttachment( 0, 0 );
    fdlAvailable.top = new FormAttachment( lastControl, margin );
    fdlAvailable.right = new FormAttachment( middle, -margin );
    wlAvailable.setLayoutData( fdlAvailable );
    wAvailable = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wAvailable.setText( stepname );
    props.setLook( wAvailable );
    FormData fdAvailable = new FormData();
    fdAvailable.left = new FormAttachment( middle, 0 );
    fdAvailable.top = new FormAttachment( wlAvailable, 0, SWT.CENTER );
    fdAvailable.right = new FormAttachment( 100, 0 );
    wAvailable.setLayoutData( fdAvailable );
    lastControl = wAvailable;

    Label wlResponseNs = new Label( shell, SWT.RIGHT );
    wlResponseNs.setText( BaseMessages.getString( PKG, "GetSlaveStatusDialog.ResponseNs" ) );
    props.setLook( wlResponseNs );
    FormData fdlResponseNs = new FormData();
    fdlResponseNs.left = new FormAttachment( 0, 0 );
    fdlResponseNs.top = new FormAttachment( lastControl, margin );
    fdlResponseNs.right = new FormAttachment( middle, -margin );
    wlResponseNs.setLayoutData( fdlResponseNs );
    wResponseNs = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResponseNs.setText( stepname );
    props.setLook( wResponseNs );
    FormData fdResponseNs = new FormData();
    fdResponseNs.left = new FormAttachment( middle, 0 );
    fdResponseNs.top = new FormAttachment( wlResponseNs, 0, SWT.CENTER );
    fdResponseNs.right = new FormAttachment( 100, 0 );
    wResponseNs.setLayoutData( fdResponseNs );
    lastControl = wResponseNs;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, lastControl );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wSlaveField.addSelectionListener( lsDef );
    wErrorMessage.addSelectionListener( lsDef );
    wStatusDescription.addSelectionListener( lsDef );
    wServerLoad.addSelectionListener( lsDef );
    wMemoryFree.addSelectionListener( lsDef );
    wMemoryTotal.addSelectionListener( lsDef );
    wCpuCores.addSelectionListener( lsDef );
    wCpuProcessTime.addSelectionListener( lsDef );
    wOsName.addSelectionListener( lsDef );
    wOsVersion.addSelectionListener( lsDef );
    wOsArchitecture.addSelectionListener( lsDef );
    wActiveTransformations.addSelectionListener( lsDef );
    wActiveJobs.addSelectionListener( lsDef );
    wAvailable.addSelectionListener( lsDef );
    wResponseNs.addSelectionListener( lsDef );

    // Get field names...
    //
    try {
      wSlaveField.setItems( transMeta.getPrevStepFields( stepname ).getFieldNames() );
    } catch(Exception e) {
      log.logError("Error getting field names", e);
    }

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData( );
    setSize();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

 

  /**
   * Populate the widgets.
   */
  public void getData( ) {
    wStepname.setText( stepname );

    wSlaveField.setText(Const.NVL(input.getSlaveField(), ""));
    wErrorMessage.setText(Const.NVL(input.getErrorMessageField(), ""));
    wStatusDescription.setText(Const.NVL(input.getStatusDescriptionField(), ""));
    wServerLoad.setText(Const.NVL(input.getServerLoadField(), ""));
    wMemoryFree.setText(Const.NVL(input.getMemoryFreeField(), ""));
    wMemoryTotal.setText(Const.NVL(input.getMemoryTotalField(), ""));
    wCpuCores.setText(Const.NVL(input.getCpuCoresField(), ""));
    wCpuProcessTime.setText(Const.NVL(input.getCpuProcessTimeField(), ""));
    wOsName.setText(Const.NVL(input.getOsNameField(), ""));
    wOsVersion.setText(Const.NVL(input.getOsVersionField(), ""));
    wOsArchitecture.setText(Const.NVL(input.getOsArchitectureField(), ""));
    wActiveTransformations.setText(Const.NVL(input.getActiveTransformationsField(), ""));
    wActiveJobs.setText(Const.NVL(input.getActiveJobsField(), ""));
    wAvailable.setText(Const.NVL(input.getAvailableField(), ""));
    wResponseNs.setText(Const.NVL(input.getResponseNsField(), ""));

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    getInfo( input );
    dispose();
  }

  private void getInfo( GetSlaveStatusMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setSlaveField(wSlaveField.getText());
    in.setErrorMessageField(wErrorMessage.getText());
    in.setStatusDescriptionField(wStatusDescription.getText());
    in.setServerLoadField(wServerLoad.getText());
    in.setMemoryFreeField(wMemoryFree.getText());
    in.setMemoryTotalField(wMemoryTotal.getText());
    in.setCpuCoresField(wCpuCores.getText());
    in.setCpuProcessTimeField(wCpuProcessTime.getText());
    in.setOsNameField(wOsName.getText());
    in.setOsVersionField(wOsVersion.getText());
    in.setOsArchitectureField(wOsArchitecture.getText());
    in.setActiveTransformationsField(wActiveTransformations.getText());
    in.setActiveJobsField(wActiveJobs.getText());
    in.setAvailableField(wAvailable.getText());
    in.setResponseNsField(wResponseNs.getText());

    in.setChanged();
  }
}