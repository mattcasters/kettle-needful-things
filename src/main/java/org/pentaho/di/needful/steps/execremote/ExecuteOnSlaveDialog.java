
package org.pentaho.di.needful.steps.execremote;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.needful.shared.ParameterDetails;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


public class ExecuteOnSlaveDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = ExecuteOnSlave.class; // for i18n purposes, needed by Translator2!!
  private final ExecuteOnSlaveMeta input;

  int middle;
  int margin;

  private boolean getpreviousFields = false;

  private CCombo wSlaveField;
  private CCombo wFilenameField;
  private Text wResultXmlField;
  private Button wExportResources;
  private Button wNotWaiting;
  private Button wReportRemote;
  private TableView wParameters;

  public ExecuteOnSlaveDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (ExecuteOnSlaveMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    String fieldNames[];
    try {
      fieldNames = transMeta.getPrevStepFields( stepMeta ).getFieldNames();
    } catch(Exception e) {
      fieldNames = new String[] {};
    }

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
    wlSlaveField.setText( BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.SlaveField" ) );
    props.setLook( wlSlaveField );
    FormData fdlSlaveField = new FormData();
    fdlSlaveField.left = new FormAttachment( 0, 0 );
    fdlSlaveField.top = new FormAttachment( lastControl, margin );
    fdlSlaveField.right = new FormAttachment( middle, -margin );
    wlSlaveField.setLayoutData( fdlSlaveField );
    wSlaveField = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSlaveField.setItems( fieldNames );
    props.setLook( wSlaveField );
    FormData fdSlaveField = new FormData();
    fdSlaveField.left = new FormAttachment( middle, 0 );
    fdSlaveField.top = new FormAttachment( wlSlaveField, 0, SWT.CENTER );
    fdSlaveField.right = new FormAttachment( 100, 0 );
    wSlaveField.setLayoutData( fdSlaveField );
    lastControl = wSlaveField;

    Label wlFilenameField = new Label( shell, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.FilenameField" ) );
    props.setLook( wlFilenameField );
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment( 0, 0 );
    fdlFilenameField.top = new FormAttachment( lastControl, margin );
    fdlFilenameField.right = new FormAttachment( middle, -margin );
    wlFilenameField.setLayoutData( fdlFilenameField );
    wFilenameField = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wFilenameField.setItems( fieldNames );
    props.setLook( wFilenameField );
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment( middle, 0 );
    fdFilenameField.top = new FormAttachment( wlFilenameField, 0, SWT.CENTER );
    fdFilenameField.right = new FormAttachment( 100, 0 );
    wFilenameField.setLayoutData( fdFilenameField );
    lastControl = wFilenameField;

    Label wlExportResources = new Label( shell, SWT.RIGHT );
    wlExportResources.setText( BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.ExportResources" ) );
    props.setLook( wlExportResources );
    FormData fdlExportResources = new FormData();
    fdlExportResources.left = new FormAttachment( 0, 0 );
    fdlExportResources.top = new FormAttachment( lastControl, margin );
    fdlExportResources.right = new FormAttachment( middle, -margin );
    wlExportResources.setLayoutData( fdlExportResources );
    wExportResources = new Button( shell, SWT.CHECK );
    props.setLook( wExportResources );
    FormData fdExportResources = new FormData();
    fdExportResources.left = new FormAttachment( middle, 0 );
    fdExportResources.top = new FormAttachment( wlExportResources, 0, SWT.CENTER );
    fdExportResources.right = new FormAttachment( 100, 0 );
    wExportResources.setLayoutData( fdExportResources );
    lastControl = wExportResources;

    Label wlNotWaiting = new Label( shell, SWT.RIGHT );
    wlNotWaiting.setText( BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.NotWaiting" ) );
    props.setLook( wlNotWaiting );
    FormData fdlNotWaiting = new FormData();
    fdlNotWaiting.left = new FormAttachment( 0, 0 );
    fdlNotWaiting.top = new FormAttachment( lastControl, margin );
    fdlNotWaiting.right = new FormAttachment( middle, -margin );
    wlNotWaiting.setLayoutData( fdlNotWaiting );
    wNotWaiting = new Button( shell, SWT.CHECK );
    props.setLook( wNotWaiting );
    FormData fdNotWaiting = new FormData();
    fdNotWaiting.left = new FormAttachment( middle, 0 );
    fdNotWaiting.top = new FormAttachment( wlNotWaiting, 0, SWT.CENTER );
    fdNotWaiting.right = new FormAttachment( 100, 0 );
    wNotWaiting.setLayoutData( fdNotWaiting );
    lastControl = wNotWaiting;

    Label wlReportRemote = new Label( shell, SWT.RIGHT );
    wlReportRemote.setText( BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.ReportRemote" ) );
    props.setLook( wlReportRemote );
    FormData fdlReportRemote = new FormData();
    fdlReportRemote.left = new FormAttachment( 0, 0 );
    fdlReportRemote.top = new FormAttachment( lastControl, margin );
    fdlReportRemote.right = new FormAttachment( middle, -margin );
    wlReportRemote.setLayoutData( fdlReportRemote );
    wReportRemote = new Button( shell, SWT.CHECK );
    props.setLook( wReportRemote );
    FormData fdReportRemote = new FormData();
    fdReportRemote.left = new FormAttachment( middle, 0 );
    fdReportRemote.top = new FormAttachment( wlReportRemote, 0, SWT.CENTER );
    fdReportRemote.right = new FormAttachment( 100, 0 );
    wReportRemote.setLayoutData( fdReportRemote );
    lastControl = wReportRemote;
    
    Label wlResultXmlField = new Label( shell, SWT.RIGHT );
    wlResultXmlField.setText( BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.ResultXmlField" ) );
    props.setLook( wlResultXmlField );
    FormData fdlResultXmlField = new FormData();
    fdlResultXmlField.left = new FormAttachment( 0, 0 );
    fdlResultXmlField.top = new FormAttachment( lastControl, margin );
    fdlResultXmlField.right = new FormAttachment( middle, -margin );
    wlResultXmlField.setLayoutData( fdlResultXmlField );
    wResultXmlField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResultXmlField );
    FormData fdResultXmlField = new FormData();
    fdResultXmlField.left = new FormAttachment( middle, 0 );
    fdResultXmlField.top = new FormAttachment( wlResultXmlField, 0, SWT.CENTER );
    fdResultXmlField.right = new FormAttachment( 100, 0 );
    wResultXmlField.setLayoutData( fdResultXmlField );
    lastControl = wResultXmlField;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    // Parameters
    //
    Label wlParameters = new Label( shell, SWT.LEFT );
    wlParameters.setText( BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.Parameters" ) );
    props.setLook( wlParameters );
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment( 0, 0 );
    fdlParameters.top = new FormAttachment( lastControl, margin );
    fdlParameters.right = new FormAttachment( 100, 0 );
    wlParameters.setLayoutData( fdlParameters );
    lastControl = wlParameters;

    ColumnInfo[] columnInfos = new ColumnInfo[] {
      new ColumnInfo(BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.Parameters.Column.Name" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false),
      new ColumnInfo(BaseMessages.getString( PKG, "ExecuteOnSlaveDialog.Parameters.Column.Field" ), ColumnInfo.COLUMN_TYPE_CCOMBO,fieldNames ),

    };
    wParameters = new TableView( transMeta, shell, SWT.BORDER, columnInfos, input.getParameters().size(), null, props );
    props.setLook( wParameters );
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment( 0, 0 );
    fdParameters.right = new FormAttachment( 100, 0 );
    fdParameters.top = new FormAttachment( lastControl, margin );
    fdParameters.bottom = new FormAttachment( wOK, -margin*2 );
    wParameters.setLayoutData( fdParameters );
    lastControl = wParameters;


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
    wFilenameField.addSelectionListener( lsDef );
    wResultXmlField.addSelectionListener( lsDef );

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
    wFilenameField.setText(Const.NVL(input.getFilenameField(), ""));
    wResultXmlField.setText(Const.NVL(input.getResultXmlField(), ""));
    wExportResources.setSelection( input.isExportingResources() );
    wNotWaiting.setSelection( input.isNotWaiting() );
    wReportRemote.setSelection( input.isReportingRemoteLog() );

    int rowNr=0;
    for ( ParameterDetails parameter : input.getParameters()) {
      TableItem item = wParameters.table.getItem( rowNr++ );
      item.setText( 1, Const.NVL(parameter.getName(), "") );
      item.setText( 2, Const.NVL(parameter.getField(), "") );
    }
    wParameters.setRowNums();
    wParameters.optWidth( true );

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

  private void getInfo( ExecuteOnSlaveMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setSlaveField(wSlaveField.getText());
    in.setFilenameField( wFilenameField.getText());
    in.setExportingResources(wExportResources.getSelection());
    in.setNotWaiting( wNotWaiting.getSelection() );
    in.setReportingRemoteLog( wReportRemote.getSelection() );
    in.setResultXmlField(wResultXmlField.getText());
    in.getParameters().clear();
    for (int i=0;i<wParameters.nrNonEmpty();i++) {
      TableItem item = wParameters.getNonEmpty( i );
      in.getParameters().add(new ParameterDetails( item.getText(1), item.getText(2) ));
    }

    input.setChanged();
  }
}