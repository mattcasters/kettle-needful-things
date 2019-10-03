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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
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
  private Button wKeepValues;
  private TableView wParameters;

  private Group wLogFileGroup;
  private Button wLogFileEnabled;
  private Label wlLogFileBase;
  private TextVar wLogFileBase;
  private Label wlLogFileExtension;
  private TextVar wLogFileExtension;
  private Label wlLogFileDateAdded;
  private Button wLogFileDateAdded;
  private Label wlLogFileTimeAdded;
  private Button wLogFileTimeAdded;
  private Label wlLogFileRepetitionAdded;
  private Button wLogFileRepetitionAdded;
  private Label wlLogFileAppended;
  private Button wLogFileAppended;
  private Label wlLogFileUpdateInterval;
  private TextVar wLogFileUpdateInterval;

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
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( 100, 0 );
    fdFilename.top = new FormAttachment( wlFilename, 0, SWT.CENTER );
    wFilename.setLayoutData( fdFilename );
    lastControl = wFilename;

    Label wlVariableName = new Label( shell, SWT.RIGHT );
    wlVariableName.setText( "Stop repeating when this variable is set" );
    props.setLook( wlVariableName );
    FormData fdlVariableName = new FormData();
    fdlVariableName.left = new FormAttachment( 0, 0 );
    fdlVariableName.right = new FormAttachment( middle, -margin );
    fdlVariableName.top = new FormAttachment( lastControl, margin );
    wlVariableName.setLayoutData( fdlVariableName );
    wVariableName = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wVariableName );
    FormData fdVariableName = new FormData();
    fdVariableName.left = new FormAttachment( middle, 0 );
    fdVariableName.right = new FormAttachment( 100, 0 );
    fdVariableName.top = new FormAttachment( wlVariableName, 0, SWT.CENTER );
    wVariableName.setLayoutData( fdVariableName );
    lastControl = wVariableName;

    Label wlVariableValue = new Label( shell, SWT.RIGHT );
    wlVariableValue.setText( "Optional variable value " );
    props.setLook( wlVariableValue );
    FormData fdlVariableValue = new FormData();
    fdlVariableValue.left = new FormAttachment( 0, 0 );
    fdlVariableValue.right = new FormAttachment( middle, -margin );
    fdlVariableValue.top = new FormAttachment( lastControl, margin );
    wlVariableValue.setLayoutData( fdlVariableValue );
    wVariableValue = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wVariableValue );
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
    FormData fdDelay = new FormData();
    fdDelay.left = new FormAttachment( middle, 0 );
    fdDelay.right = new FormAttachment( 100, 0 );
    fdDelay.top = new FormAttachment( wlDelay, 0, SWT.CENTER );
    wDelay.setLayoutData( fdDelay );
    lastControl = wDelay;

    Label wlKeepValues = new Label( shell, SWT.RIGHT );
    wlKeepValues.setText( "Keep variable values after executions " );
    props.setLook( wlKeepValues );
    FormData fdlKeepValues = new FormData();
    fdlKeepValues.left = new FormAttachment( 0, 0 );
    fdlKeepValues.right = new FormAttachment( middle, -margin );
    fdlKeepValues.top = new FormAttachment( lastControl, margin );
    wlKeepValues.setLayoutData( fdlKeepValues );
    wKeepValues = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wKeepValues );
    FormData fdKeepValues = new FormData();
    fdKeepValues.left = new FormAttachment( middle, 0 );
    fdKeepValues.right = new FormAttachment( 100, 0 );
    fdKeepValues.top = new FormAttachment( wlKeepValues, 0, SWT.CENTER );
    wKeepValues.setLayoutData( fdKeepValues );
    lastControl = wKeepValues;

    wLogFileGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wLogFileGroup);
    wLogFileGroup.setText("Logging file");
    FormLayout logFileGroupLayout = new FormLayout();
    logFileGroupLayout.marginLeft = Const.MARGIN;
    logFileGroupLayout.marginRight = Const.MARGIN;
    logFileGroupLayout.marginTop = 2*Const.MARGIN;
    logFileGroupLayout.marginBottom = 2*Const.MARGIN;
    wLogFileGroup.setLayout( logFileGroupLayout );

    Label wlLogFileEnabled = new Label( wLogFileGroup, SWT.RIGHT );
    wlLogFileEnabled.setText( "Log the execution to a file? " );
    props.setLook( wlLogFileEnabled );
    FormData fdlLogFileEnabled = new FormData();
    fdlLogFileEnabled.left = new FormAttachment( 0, 0 );
    fdlLogFileEnabled.right = new FormAttachment( middle, -margin );
    fdlLogFileEnabled.top = new FormAttachment( 0, 0 );
    wlLogFileEnabled.setLayoutData( fdlLogFileEnabled );
    wLogFileEnabled = new Button( wLogFileGroup, SWT.CHECK | SWT.LEFT );
    props.setLook( wLogFileEnabled );
    FormData fdLogFileEnabled = new FormData();
    fdLogFileEnabled.left = new FormAttachment( middle, 0 );
    fdLogFileEnabled.right = new FormAttachment( 100, 0 );
    fdLogFileEnabled.top = new FormAttachment( wlLogFileEnabled, 0, SWT.CENTER );
    wLogFileEnabled.setLayoutData( fdLogFileEnabled );
    wLogFileEnabled.addListener( SWT.Selection, e-> enableControls() );
    Control lastLogControl = wLogFileEnabled;

    wlLogFileBase = new Label( wLogFileGroup, SWT.RIGHT );
    wlLogFileBase.setText( "The base log file name " );
    props.setLook( wlLogFileBase );
    FormData fdlLogFileBase = new FormData();
    fdlLogFileBase.left = new FormAttachment( 0, 0 );
    fdlLogFileBase.right = new FormAttachment( middle, -margin );
    fdlLogFileBase.top = new FormAttachment( lastLogControl, margin );
    wlLogFileBase.setLayoutData( fdlLogFileBase );
    wLogFileBase = new TextVar( jobMeta, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLogFileBase );
    FormData fdLogFileBase = new FormData();
    fdLogFileBase.left = new FormAttachment( middle, 0 );
    fdLogFileBase.right = new FormAttachment( 100, 0 );
    fdLogFileBase.top = new FormAttachment( wlLogFileBase, 0, SWT.CENTER );
    wLogFileBase.setLayoutData( fdLogFileBase );
    lastLogControl = wLogFileBase;

    wlLogFileExtension = new Label( wLogFileGroup, SWT.RIGHT );
    wlLogFileExtension.setText( "The log file extension " );
    props.setLook( wlLogFileExtension );
    FormData fdlLogFileExtension = new FormData();
    fdlLogFileExtension.left = new FormAttachment( 0, 0 );
    fdlLogFileExtension.right = new FormAttachment( middle, -margin );
    fdlLogFileExtension.top = new FormAttachment( lastLogControl, margin );
    wlLogFileExtension.setLayoutData( fdlLogFileExtension );
    wLogFileExtension = new TextVar( jobMeta, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLogFileExtension );
    FormData fdLogFileExtension = new FormData();
    fdLogFileExtension.left = new FormAttachment( middle, 0 );
    fdLogFileExtension.right = new FormAttachment( 100, 0 );
    fdLogFileExtension.top = new FormAttachment( wlLogFileExtension, 0, SWT.CENTER );
    wLogFileExtension.setLayoutData( fdLogFileExtension );
    lastLogControl = wLogFileExtension;

    wlLogFileDateAdded = new Label( wLogFileGroup, SWT.RIGHT );
    wlLogFileDateAdded.setText( "Add the date to the filename? " );
    props.setLook( wlLogFileDateAdded );
    FormData fdlLogFileDateAdded = new FormData();
    fdlLogFileDateAdded.left = new FormAttachment( 0, 0 );
    fdlLogFileDateAdded.right = new FormAttachment( middle, -margin );
    fdlLogFileDateAdded.top = new FormAttachment( lastLogControl, margin );
    wlLogFileDateAdded.setLayoutData( fdlLogFileDateAdded );
    wLogFileDateAdded = new Button( wLogFileGroup, SWT.CHECK | SWT.LEFT );
    props.setLook( wLogFileDateAdded );
    FormData fdLogFileDateAdded = new FormData();
    fdLogFileDateAdded.left = new FormAttachment( middle, 0 );
    fdLogFileDateAdded.right = new FormAttachment( 100, 0 );
    fdLogFileDateAdded.top = new FormAttachment( wlLogFileDateAdded, 0, SWT.CENTER );
    wLogFileDateAdded.setLayoutData( fdLogFileDateAdded );
    lastLogControl = wLogFileDateAdded;

    wlLogFileTimeAdded = new Label( wLogFileGroup, SWT.RIGHT );
    wlLogFileTimeAdded.setText( "Add the time to the filename? " );
    props.setLook( wlLogFileTimeAdded );
    FormData fdlLogFileTimeAdded = new FormData();
    fdlLogFileTimeAdded.left = new FormAttachment( 0, 0 );
    fdlLogFileTimeAdded.right = new FormAttachment( middle, -margin );
    fdlLogFileTimeAdded.top = new FormAttachment( lastLogControl, margin );
    wlLogFileTimeAdded.setLayoutData( fdlLogFileTimeAdded );
    wLogFileTimeAdded = new Button( wLogFileGroup, SWT.CHECK | SWT.LEFT );
    props.setLook( wLogFileTimeAdded );
    FormData fdLogFileTimeAdded = new FormData();
    fdLogFileTimeAdded.left = new FormAttachment( middle, 0 );
    fdLogFileTimeAdded.right = new FormAttachment( 100, 0 );
    fdLogFileTimeAdded.top = new FormAttachment( wlLogFileTimeAdded, 0, SWT.CENTER );
    wLogFileTimeAdded.setLayoutData( fdLogFileTimeAdded );
    lastLogControl = wLogFileTimeAdded;

    wlLogFileRepetitionAdded = new Label( wLogFileGroup, SWT.RIGHT );
    wlLogFileRepetitionAdded.setText( "Add the repetition number to the filename? " );
    props.setLook( wlLogFileRepetitionAdded );
    FormData fdlLogFileRepetitionAdded = new FormData();
    fdlLogFileRepetitionAdded.left = new FormAttachment( 0, 0 );
    fdlLogFileRepetitionAdded.right = new FormAttachment( middle, -margin );
    fdlLogFileRepetitionAdded.top = new FormAttachment( lastLogControl, margin );
    wlLogFileRepetitionAdded.setLayoutData( fdlLogFileRepetitionAdded );
    wLogFileRepetitionAdded = new Button( wLogFileGroup, SWT.CHECK | SWT.LEFT );
    props.setLook( wLogFileRepetitionAdded );
    FormData fdLogFileRepetitionAdded = new FormData();
    fdLogFileRepetitionAdded.left = new FormAttachment( middle, 0 );
    fdLogFileRepetitionAdded.right = new FormAttachment( 100, 0 );
    fdLogFileRepetitionAdded.top = new FormAttachment( wlLogFileRepetitionAdded, 0, SWT.CENTER );
    wLogFileRepetitionAdded.setLayoutData( fdLogFileRepetitionAdded );
    lastLogControl = wLogFileRepetitionAdded;

    wlLogFileAppended = new Label( wLogFileGroup, SWT.RIGHT );
    wlLogFileAppended.setText( "Append to any existing log file? " );
    props.setLook( wlLogFileAppended );
    FormData fdlLogFileAppended = new FormData();
    fdlLogFileAppended.left = new FormAttachment( 0, 0 );
    fdlLogFileAppended.right = new FormAttachment( middle, -margin );
    fdlLogFileAppended.top = new FormAttachment( lastLogControl, margin );
    wlLogFileAppended.setLayoutData( fdlLogFileAppended );
    wLogFileAppended = new Button( wLogFileGroup, SWT.CHECK | SWT.LEFT );
    props.setLook( wLogFileAppended );
    FormData fdLogFileAppended = new FormData();
    fdLogFileAppended.left = new FormAttachment( middle, 0 );
    fdLogFileAppended.right = new FormAttachment( 100, 0 );
    fdLogFileAppended.top = new FormAttachment( wlLogFileAppended, 0, SWT.CENTER );
    wLogFileAppended.setLayoutData( fdLogFileAppended );
    lastLogControl = wLogFileAppended;

    wlLogFileUpdateInterval = new Label( wLogFileGroup, SWT.RIGHT );
    wlLogFileUpdateInterval.setText( "The log file update interval in ms " );
    props.setLook( wlLogFileUpdateInterval );
    FormData fdlLogFileUpdateInterval = new FormData();
    fdlLogFileUpdateInterval.left = new FormAttachment( 0, 0 );
    fdlLogFileUpdateInterval.right = new FormAttachment( middle, -margin );
    fdlLogFileUpdateInterval.top = new FormAttachment( lastLogControl, margin );
    wlLogFileUpdateInterval.setLayoutData( fdlLogFileUpdateInterval );
    wLogFileUpdateInterval = new TextVar( jobMeta, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLogFileUpdateInterval );
    FormData fdLogFileUpdateInterval = new FormData();
    fdLogFileUpdateInterval.left = new FormAttachment( middle, 0 );
    fdLogFileUpdateInterval.right = new FormAttachment( 100, 0 );
    fdLogFileUpdateInterval.top = new FormAttachment( wlLogFileUpdateInterval, 0, SWT.CENTER );
    wLogFileUpdateInterval.setLayoutData( fdLogFileUpdateInterval );
    lastLogControl = wLogFileUpdateInterval;


    FormData fdLogFileGroup = new FormData(  );
    fdLogFileGroup.left = new FormAttachment( 0, 0 );
    fdLogFileGroup.right = new FormAttachment( 100, 0 );
    fdLogFileGroup.top = new FormAttachment( lastControl, margin );
    wLogFileGroup.setLayoutData( fdLogFileGroup );
    wLogFileGroup.pack();
    lastControl = wLogFileGroup;

    // Parameters
    //
    Label wlParameters = new Label( shell, SWT.LEFT );
    wlParameters.setText( "Parameters/Variables to set: " );
    props.setLook( wlParameters );
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment( 0, 0 );
    fdlParameters.top = new FormAttachment( lastControl, 2*margin );
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
    columnInfos[1].setUsingVariables( true );

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

  private void enableControls() {
    boolean logEnabled = wLogFileEnabled.getSelection();

    wlLogFileBase.setEnabled(logEnabled);
    wLogFileBase.setEnabled(logEnabled);
    wlLogFileExtension.setEnabled(logEnabled);
    wLogFileExtension.setEnabled(logEnabled);
    wlLogFileDateAdded.setEnabled(logEnabled);
    wLogFileDateAdded.setEnabled(logEnabled);
    wlLogFileTimeAdded.setEnabled(logEnabled);
    wLogFileTimeAdded.setEnabled(logEnabled);
    wlLogFileRepetitionAdded.setEnabled(logEnabled);
    wLogFileRepetitionAdded.setEnabled(logEnabled);
    wlLogFileAppended.setEnabled(logEnabled);
    wLogFileAppended.setEnabled(logEnabled);
    wlLogFileUpdateInterval.setEnabled(logEnabled);
    wLogFileUpdateInterval.setEnabled(logEnabled);
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
    wKeepValues.setSelection(jobEntry.isKeepingValues());

    wLogFileEnabled.setSelection( jobEntry.isLogFileEnabled() );
    wLogFileBase.setText(Const.NVL(jobEntry.getLogFileBase(),""));
    wLogFileExtension.setText(Const.NVL(jobEntry.getLogFileExtension(), ""));
    wLogFileDateAdded.setSelection( jobEntry.isLogFileDateAdded() );
    wLogFileTimeAdded.setSelection( jobEntry.isLogFileTimeAdded());
    wLogFileRepetitionAdded.setSelection( jobEntry.isLogFileRepetitionAdded() );
    wLogFileAppended.setSelection( jobEntry.isLogFileAppended() );
    wLogFileUpdateInterval.setText(Const.NVL(jobEntry.getLogFileUpdateInterval(), "5000"));

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

    enableControls();
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
    jobEntry.setVariableName( wVariableName.getText() );
    jobEntry.setVariableValue( wVariableValue.getText() );
    jobEntry.setDelay( wDelay.getText() );
    jobEntry.setKeepingValues( wKeepValues.getSelection() );

    jobEntry.setLogFileEnabled( wLogFileEnabled.getSelection() );
    jobEntry.setLogFileAppended( wLogFileAppended.getSelection() );
    jobEntry.setLogFileBase( wLogFileBase.getText() );
    jobEntry.setLogFileExtension( wLogFileExtension.getText() );
    jobEntry.setLogFileDateAdded( wLogFileDateAdded.getSelection() );
    jobEntry.setLogFileTimeAdded( wLogFileTimeAdded.getSelection() );
    jobEntry.setLogFileRepetitionAdded( wLogFileRepetitionAdded.getSelection() );
    jobEntry.setLogFileUpdateInterval( wLogFileUpdateInterval.getText() );

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
