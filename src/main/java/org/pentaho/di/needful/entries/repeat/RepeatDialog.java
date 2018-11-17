package org.pentaho.di.needful.entries.repeat;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.needful.shared.ParameterDetails;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class RepeatDialog extends JobEntryDialog implements JobEntryDialogInterface {

  private static Class<?> PKG = RepeatDialog.class; // for i18n purposes, needed by Translator2!!

  private Shell shell;

  private Repeat jobEntry;

  private Text wName;
  private TextVar wFilename;
  private TextVar wVariableName;
  private TextVar wVariableValue;
  private TextVar wDelay;
  private TableView wParameters;

  private Button wOK, wCancel;

  public RepeatDialog( Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntry, rep, jobMeta );
    this.jobEntry = (Repeat) jobEntry;

    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( "Repeat" );
    }
  }

  @Override public JobEntryInterface open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    
    shell.setLayout( formLayout );
    shell.setText( "Repeat" );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    Label wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( "Job entry name" );
    props.setLook( wlName );
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;
    
    Label wlFilename = new Label( shell, SWT.RIGHT );
    wlFilename.setText( "File to repeat (.ktr or .kjb) " );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.right = new FormAttachment( middle, -margin );
    fdlFilename.top = new FormAttachment( lastControl, margin );
    wlFilename.setLayoutData( fdlFilename );
    wFilename = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( 100, 0 );
    fdFilename.top = new FormAttachment( wlFilename, 0, SWT.CENTER );
    wFilename.setLayoutData( fdFilename );
    lastControl = wFilename;

    Label wlVariableName = new Label( shell, SWT.RIGHT );
    wlVariableName.setText( "Variable when set exiting loop " );
    props.setLook( wlVariableName );
    FormData fdlVariableName = new FormData();
    fdlVariableName.left = new FormAttachment( 0, 0 );
    fdlVariableName.right = new FormAttachment( middle, -margin );
    fdlVariableName.top = new FormAttachment( lastControl, margin );
    wlVariableName.setLayoutData( fdlVariableName );
    wVariableName = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wVariableName );
    wVariableName.addModifyListener( lsMod );
    FormData fdVariableName = new FormData();
    fdVariableName.left = new FormAttachment( middle, 0 );
    fdVariableName.right = new FormAttachment( 100, 0 );
    fdVariableName.top = new FormAttachment( wlVariableName, 0, SWT.CENTER );
    wVariableName.setLayoutData( fdVariableName );
    lastControl = wVariableName;

    Label wlVariableValue = new Label( shell, SWT.RIGHT );
    wlVariableValue.setText( "Variable when set exiting loop " );
    props.setLook( wlVariableValue );
    FormData fdlVariableValue = new FormData();
    fdlVariableValue.left = new FormAttachment( 0, 0 );
    fdlVariableValue.right = new FormAttachment( middle, -margin );
    fdlVariableValue.top = new FormAttachment( lastControl, margin );
    wlVariableValue.setLayoutData( fdlVariableValue );
    wVariableValue = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wVariableValue );
    wVariableValue.addModifyListener( lsMod );
    FormData fdVariableValue = new FormData();
    fdVariableValue.left = new FormAttachment( middle, 0 );
    fdVariableValue.right = new FormAttachment( 100, 0 );
    fdVariableValue.top = new FormAttachment( wlVariableValue, 0, SWT.CENTER );
    wVariableValue.setLayoutData( fdVariableValue );
    lastControl = wVariableValue;

    Label wlDelay = new Label( shell, SWT.RIGHT );
    wlDelay.setText( "Delay in seconds " );
    props.setLook( wlDelay );
    FormData fdlDelay = new FormData();
    fdlDelay.left = new FormAttachment( 0, 0 );
    fdlDelay.right = new FormAttachment( middle, -margin );
    fdlDelay.top = new FormAttachment( lastControl, margin );
    wlDelay.setLayoutData( fdlDelay );
    wDelay = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDelay );
    wDelay.addModifyListener( lsMod );
    FormData fdDelay = new FormData();
    fdDelay.left = new FormAttachment( middle, 0 );
    fdDelay.right = new FormAttachment( 100, 0 );
    fdDelay.top = new FormAttachment( wlDelay, 0, SWT.CENTER );
    wDelay.setLayoutData( fdDelay );
    lastControl = wDelay;

    // Parameters
    //
    Label wlParameters = new Label( shell, SWT.LEFT );
    wlParameters.setText( "Parameters/Variables to set: " );
    props.setLook( wlParameters );
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment( 0, 0 );
    fdlParameters.top = new FormAttachment( lastControl, margin );
    fdlParameters.right = new FormAttachment( 100, 0 );
    wlParameters.setLayoutData( fdlParameters );
    lastControl = wlParameters;

    // Add buttons first, then the script field can use dynamic sizing
    //
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, e   -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    // Put these buttons at the bottom
    //
    BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel, }, margin, null );

    ColumnInfo[] columnInfos = new ColumnInfo[] {
      new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
      new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
    };

    wParameters = new TableView( jobMeta, shell, SWT.BORDER, columnInfos, jobEntry.getParameters().size(), null, props );
    props.setLook( wParameters );
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment( 0, 0 );
    fdParameters.right = new FormAttachment( 100, 0 );
    fdParameters.top = new FormAttachment( lastControl, margin );
    fdParameters.bottom = new FormAttachment( wOK, -margin*2 );
    wParameters.setLayoutData( fdParameters );
    lastControl = wParameters;

    // Detect X or ALT-F4 or something that kills this window...
    //
    shell.addListener( SWT.Close, e -> cancel() );
    wName.addListener( SWT.DefaultSelection, e -> ok() );
    wFilename.addListener( SWT.DefaultSelection, e -> ok() );
    wVariableName.addListener( SWT.DefaultSelection, e -> ok() );
    wVariableValue.addListener( SWT.DefaultSelection, e -> ok() );
    wDelay.addListener( SWT.DefaultSelection, e -> ok() );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return jobEntry;
  }

  private void cancel() {
    jobEntry = null;
    dispose();
  }

  private void getData() {
    wName.setText( Const.NVL(jobEntry.getName(), "") );
    wFilename.setText( Const.NVL(jobEntry.getFilename(), ""));
    wVariableName.setText( Const.NVL(jobEntry.getVariableName(), ""));
    wVariableValue.setText( Const.NVL(jobEntry.getVariableValue(), ""));
    wDelay.setText( Const.NVL(jobEntry.getDelay(), ""));

    int rowNr=0;
    for ( ParameterDetails parameter : jobEntry.getParameters()) {
      TableItem item = wParameters.table.getItem( rowNr++ );
      item.setText( 1, Const.NVL(parameter.getName(), "") );
      item.setText( 2, Const.NVL(parameter.getField(), "") );
    }
    wParameters.setRowNums();
    wParameters.optWidth( true );

    wName.selectAll();
    wName.setFocus();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( "Warning" );
      mb.setMessage( "The name of the job entry is missing!" );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setFilename( wFilename.getText() );
    jobEntry.setVariableName( wVariableValue.getText() );
    jobEntry.setVariableValue( wVariableName.getText() );
    jobEntry.setDelay( wDelay.getText() );

    jobEntry.getParameters().clear();
    for (int i=0;i<wParameters.nrNonEmpty();i++) {
      TableItem item = wParameters.getNonEmpty( i );
      jobEntry.getParameters().add(new ParameterDetails( item.getText(1), item.getText(2) ));
    }

    jobEntry.setChanged();
    dispose();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }


}
