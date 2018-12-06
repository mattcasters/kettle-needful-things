
package org.pentaho.di.needful.steps.multistreamlookup;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.EnterStringDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.TreeMemory;
import org.pentaho.di.ui.core.widget.TreeUtil;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.List;

public class MultiStreamLookupDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = MultiStreamLookup.class; // for i18n purposes, needed by Translator2!!
  private final MultiStreamLookupMeta input;
  private final List<LookupAction> lookupActions;

  int middle;
  int margin;

  private Tree wTree;

  private String treeName;

  public MultiStreamLookupDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (MultiStreamLookupMeta) in;

    lookupActions = ( (MultiStreamLookupMeta) in ).clone().getLookupActions();
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
    shell.setText( "Multi Stream Lookup" );

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

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wAddAction = new Button( shell, SWT.PUSH );
    wAddAction.setText( "Add Lookup" );
    Button wDelAction = new Button( shell, SWT.PUSH );
    wDelAction.setText( "Delete lookup" );
    Button wAddResult = new Button( shell, SWT.PUSH );
    wAddResult.setText( "Add Result" );
    Button wDelResult = new Button( shell, SWT.PUSH );
    wDelResult.setText( "Delete Result" );
    Button wRenResult = new Button( shell, SWT.PUSH );
    wRenResult.setText( "Rename Result" );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wAddAction, wDelAction, wAddResult, wRenResult, wDelResult, wCancel }, margin, null );

    wTree = new Tree( shell, SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL | SWT.SINGLE );
    props.setLook( wTree );
    wTree.setHeaderVisible( true );
    treeName = "MultiStreamLookupDialog " + stepname;
    TreeMemory.addTreeListener( wTree, treeName );

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "Input field" );
      column.setWidth( 300 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "Dataset field" );
      column.setWidth( 300 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "Return from dataset" );
      column.setWidth( 300 );
    }

    {
      TreeColumn column = new TreeColumn( wTree, SWT.LEFT );
      column.setText( "Rename to" );
      column.setWidth( 300 );
    }

    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment( 0, 0 );
    fdTree.right = new FormAttachment( 100, 0 );
    fdTree.top = new FormAttachment( lastControl, margin * 2 );
    fdTree.bottom = new FormAttachment( wOK, -margin * 2 );
    wTree.setLayoutData( fdTree );


    wOK.addListener( SWT.Selection, (e)->ok() );
    wCancel.addListener( SWT.Selection, (e)->cancel());
    wAddAction.addListener( SWT.Selection, this::addActions );
    wDelAction.addListener( SWT.Selection, this::deleteActions );
    wAddResult.addListener( SWT.Selection, this::addResults );
    wRenResult.addListener( SWT.Selection, this::renameResult );
    wDelResult.addListener( SWT.Selection, this::deleteResults );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Get field names...
    //
    try {
      transMeta.getPrevStepFields( stepname ).getFieldNames();
    } catch ( Exception e ) {
      log.logError( "Error getting field names", e );
    }

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
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
  public void getData() {
    wStepname.setText( stepname );

    refreshTree();

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void refreshTree() {
    // Clear out wTree
    //
    wTree.removeAll();

    for ( LookupAction lookupAction : lookupActions ) {
      System.out.println("Adding action : "+lookupAction);
      TreeItem actionItem = new TreeItem( wTree, SWT.NONE );
      actionItem.setText( 0, Const.NVL( lookupAction.getInputField(), "" ) );
      actionItem.setText( 1, Const.NVL( lookupAction.getDataSetField(), "" ) );

      for ( LookupResult lookupResult : lookupAction.getLookupResults() ) {
        System.out.println("  - Lookup result : "+lookupResult);

        TreeItem resultItem = new TreeItem( actionItem, SWT.NONE );
        resultItem.setText( 2, Const.NVL( lookupResult.getDatasetField(), "" ) );
        resultItem.setText( 3, Const.NVL( lookupResult.getRenameTo(), "" ) );
      }
    }

    TreeMemory.setExpandedFromMemory( wTree, treeName );
  }

  private void addActions(Event event) {
    try {

      List<StepMeta> previousSteps = transMeta.findPreviousSteps( stepMeta, false );// Exclude info steps
      if (previousSteps.size()==0) {
        return; // Nothing to do here.
      }

      RowMetaInterface inputRowMeta = transMeta.getStepFields(previousSteps.get(0));
      RowMetaInterface infoRowMeta = transMeta.getPrevInfoFields( stepMeta );

      MultiStreamLookupMeta copy = input.clone();
      copy.setLookupActions( lookupActions );
      copy.getFields( inputRowMeta, stepname, new RowMetaInterface[] { infoRowMeta}, null, transMeta, repository, metaStore );

      String[] stepFieldNames = inputRowMeta.getFieldNames();
      String[] infoFieldNames = infoRowMeta.getFieldNames();

      EnterMappingDialog dialog = new EnterMappingDialog(shell, stepFieldNames, infoFieldNames );
      List<SourceToTargetMapping> mappings = dialog.open();
      for (SourceToTargetMapping mapping : mappings) {
        String inputField = mapping.getSourceString( stepFieldNames);
        String dataSetField = mapping.getTargetString( infoFieldNames );
        LookupAction newAction = new LookupAction(inputField, dataSetField, new ArrayList<>());
        lookupActions.add(newAction);
        // System.out.println("Add action : "+newAction);
        // System.out.println("We now have "+lookupActions.size()+" actions");
      }
      refreshTree();
    } catch(Exception e) {
      new ErrorDialog(shell, "Error", "Error adding lookup action", e);
    }
  }

  private void deleteActions(Event event) {
    try {

      TreeItem[] selections = wTree.getSelection();
      if (selections==null) {
        return;
      }

      for (TreeItem selection : selections) {
        // Which tree item from the top?
        int actionIndex = wTree.indexOf( selection );
        LookupAction lookupAction = lookupActions.get( actionIndex );
        lookupActions.remove( lookupAction );
      }

      refreshTree();

    } catch(Exception e) {
      new ErrorDialog(shell, "Error", "Error deleting lookup action", e);
    }
  }

  private void addResults(Event event) {
    try {

      TreeItem[] selection = wTree.getSelection();
      if (selection==null || selection.length!=1) {
        return;
      }

      // Which tree item from the top?
      int actionIndex = wTree.indexOf( selection[ 0 ] );
      LookupAction lookupAction = lookupActions.get( actionIndex );
      System.out.println("Add results to action : "+lookupAction);

      String[] infoFieldNames = transMeta.getPrevInfoFields( stepMeta ).getFieldNames();

      EnterSelectionDialog dialog = new EnterSelectionDialog( shell, infoFieldNames, "Select result fields", "Select the fields to include in the step output" );
      dialog.setMulti( true );
      if (dialog.open()!=null) {
        int[] indeces = dialog.getSelectionIndeces();
        System.out.println("Found "+indeces.length+" results to add");

        for (int index : indeces) {
          String resultDataSetField = infoFieldNames[index];
          // Ask for a new name...
          //
          LookupResult lookupResult = new LookupResult( resultDataSetField, null );
          lookupAction.getLookupResults().add(lookupResult);
          System.out.println("Added lookup result : "+lookupResult);
        }
      }

      refreshTree();
    } catch(Exception e) {
      new ErrorDialog(shell, "Error", "Error adding lookup result", e);
    }
  }

  private void renameResult(Event event) {
    try {

      TreeItem[] selection = wTree.getSelection();
      if (selection==null || selection.length!=1) {
        return;
      }

      TreeItem actionItem = selection[0].getParentItem();
      if (actionItem==null) {
        // Only result items
        return;
      }

      // Which tree item from the top?
      int actionIndex = wTree.indexOf( actionItem );
      LookupAction lookupAction = lookupActions.get( actionIndex );
      System.out.println("Rename result in action : "+lookupAction);

      int resultIndex = actionItem.indexOf( selection[0] );
      LookupResult lookupResult = lookupAction.getLookupResults().get( resultIndex );

      String oldRename = lookupResult.getRenameTo();
      if (StringUtils.isEmpty(oldRename)) {
        oldRename = lookupResult.getDatasetField();
      }
      EnterStringDialog stringDialog= new EnterStringDialog( shell, oldRename, "Rename field", "Choose a new name for the lookup result data set field:", false, null );
      String renameTo = stringDialog.open();
      if (renameTo!=null) {
        if (renameTo.equals(lookupResult.getDatasetField())) {
          renameTo=null;
        }
        lookupResult.setRenameTo( renameTo );
      }

      refreshTree();
    } catch(Exception e) {
      new ErrorDialog(shell, "Error", "Error renaming lookup result", e);
    }
  }

  private void deleteResults(Event event) {
    try {

      TreeItem[] selections = wTree.getSelection();
      if (selections==null) {
        return;
      }

      for (TreeItem selection : selections) {
        // Which tree item from the top?
        //
        String[] path = ConstUI.getTreeStrings( selection );
        if (path.length==2) {
          TreeItem parent = selection.getParentItem();
          int actionIndex = wTree.indexOf( parent );
          LookupAction treeAction = lookupActions.get(actionIndex);

          int lookupResultNr = parent.indexOf( selection );
          treeAction.getLookupResults().remove(treeAction);
        }
      }

      refreshTree();

    } catch(Exception e) {
      new ErrorDialog(shell, "Error", "Error deleting lookup results", e);
    }
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

  private void getInfo( MultiStreamLookupMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setLookupActions( lookupActions );

    in.setChanged();
  }
}