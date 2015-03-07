package edu.emory.cci.aiw.dsb.waveform;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import org.protempa.BackendCloseException;
import org.protempa.DataSourceBackendCloseException;
import org.protempa.backend.annotations.BackendInfo;
import org.protempa.backend.dsb.relationaldb.ColumnSpec;
import org.protempa.backend.dsb.relationaldb.Operator;
import org.protempa.backend.dsb.relationaldb.EntitySpec;
import org.protempa.backend.dsb.relationaldb.JDBCDateTimeTimestampPositionParser;
import org.protempa.backend.dsb.relationaldb.JDBCPositionFormat;
import org.protempa.backend.dsb.relationaldb.JoinSpec;
import org.protempa.backend.dsb.relationaldb.PropertySpec;
import org.protempa.backend.dsb.relationaldb.ReferenceSpec;
import org.protempa.backend.dsb.relationaldb.RelationalDbDataSourceBackend;
import org.protempa.backend.dsb.relationaldb.StagingSpec;
import org.protempa.backend.dsb.relationaldb.mappings.DefaultMappings;
import org.protempa.backend.dsb.relationaldb.mappings.Mappings;
import org.protempa.proposition.value.AbsoluteTimeGranularity;
import org.protempa.proposition.value.ValueType;

/**
 *
 * @author Andrew Post
 */
@BackendInfo(displayName = "Waveform")
public class WaveformDataSourceBackend extends RelationalDbDataSourceBackend {

    private static final JDBCPositionFormat jdbcTimestampPositionParser
            = new JDBCDateTimeTimestampPositionParser(new Date(0L));

    private final Mappings annotationMappings;

    public WaveformDataSourceBackend() {
        this.annotationMappings = new DefaultMappings(new HashMap<Object, String>() {
            {
                put("QT Dispersion", "ECGTermsv1:ECG_000001078");
                put("QT Corrected Bazett", "ECGTermsv1:ECG_000000701");
                put("QT_Interval", "ECGTermsv1:ECG_000000682");
                put("QT Interval", "ECGTermsv1:ECG_000000682");
            }
        });
        setSchemaName("public");
        setDefaultKeyIdTable("documentrecord");
        setDefaultKeyIdColumn("subjectid");
        setDefaultKeyIdJoinKey("documentrecordid");
    }

    @Override
    protected EntitySpec[] constantSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        String schemaName = getSchemaName();
        EntitySpec[] constantSpecs = {
            new EntitySpec("Patients",
            null,
            new String[]{"Patient"},
            false,
            new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn),
            new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn)},
            null,
            null,
            new PropertySpec[]{
                new PropertySpec("patientId", null, new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn), ValueType.NOMINALVALUE)
            },
            new ReferenceSpec[]{
                new ReferenceSpec("encounters", "Encounters", new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, "documentrecordid")}, ReferenceSpec.Type.MANY),
                new ReferenceSpec("patientDetails", "Patient Details", new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, "documentrecordid")}, ReferenceSpec.Type.MANY)},
            null, null, null, null, null, null, null, null),
            new EntitySpec("Patient Details",
            null,
            new String[]{"PatientDetails"},
            true,
            new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "documentrecordid", new ColumnSpec(schemaName, "documentrecord"))),
            new ColumnSpec[]{new ColumnSpec(keyIdSchema, "documentrecord", "documentrecordid")},
            null,
            null,
            new PropertySpec[]{
                new PropertySpec("patientId", null, new ColumnSpec(schemaName, "documentrecord", "subjectid"), ValueType.NOMINALVALUE)
            },
            new ReferenceSpec[]{
                new ReferenceSpec("encounters", "Encounters", new ColumnSpec[]{new ColumnSpec(schemaName, "documentrecord", "documentrecordid")}, ReferenceSpec.Type.MANY),
                new ReferenceSpec("patient", "Patients", new ColumnSpec[]{new ColumnSpec(schemaName, "documentrecord", "subjectid")}, ReferenceSpec.Type.ONE)
            },
            null, null, null, null, null, null, null, null),};
        return constantSpecs;
    }

    @Override
    protected EntitySpec[] eventSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        String schemaName = getSchemaName();

        EntitySpec[] eventSpecs = {
            new EntitySpec("Encounters",
            null,
            new String[]{"Encounter"},
            true,
            new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "documentrecordid", new ColumnSpec(schemaName, "documentrecord", new JoinSpec("documentrecordid", "documentrecordid", new ColumnSpec(schemaName, "documentrecord"))))),
            new ColumnSpec[]{new ColumnSpec(schemaName, "documentrecord", "documentrecordid")},
            new ColumnSpec(schemaName, "documentrecord", "dateofrecording"),
            null,
            new PropertySpec[]{
                new PropertySpec("encounterId", null, new ColumnSpec(schemaName, "documentrecord", "documentrecordid"), ValueType.NOMINALVALUE),},
            new ReferenceSpec[]{
                new ReferenceSpec("patient", "Patients", new ColumnSpec[]{new ColumnSpec(schemaName, "documentrecord", "subjectid")}, ReferenceSpec.Type.ONE),
                new ReferenceSpec("CVRG_EKG_ANNOTATIONS", "EKG Annotations", new ColumnSpec[]{new ColumnSpec(schemaName, "documentrecord", new JoinSpec("documentrecordid", "documentrecordid", new ColumnSpec(schemaName, "annotationinfo", "annotationid")))}, ReferenceSpec.Type.MANY),
                new ReferenceSpec("patientDetails", "Patient Details", new ColumnSpec[]{new ColumnSpec(schemaName, "documentrecord", "documentrecordid")}, ReferenceSpec.Type.ONE),},
            null, null, null, null, null, AbsoluteTimeGranularity.MINUTE, jdbcTimestampPositionParser, null),};
        return eventSpecs;
    }

    @Override
    protected EntitySpec[] primitiveParameterSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        String schemaName = getSchemaName();

        EntitySpec[] ppSpecs = {
            new EntitySpec("EKG Annotations",
            null,
            new String[]{"ECGTermsv1:ECG_000001078", "ECGTermsv1:ECG_000000701", "ECGTermsv1:ECG_000000682"},
            true,
            new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "documentrecordid", new ColumnSpec(schemaName, "documentrecord", new JoinSpec("documentrecordid", "documentrecordid", new ColumnSpec(schemaName, "documentrecord", new JoinSpec("documentrecordid", "documentrecordid", new ColumnSpec(schemaName, "annotationinfo"))))))),
            new ColumnSpec[]{new ColumnSpec(schemaName, "annotationinfo", "annotationid")},
            new ColumnSpec(schemaName, "annotationinfo", "timestamp"),
            null,
            new PropertySpec[]{
                new PropertySpec("unitOfMeasure", null, new ColumnSpec(schemaName, "annotationinfo", "unitofmeasurement"), ValueType.NOMINALVALUE)
            },
            null,
            null,
            new ColumnSpec(schemaName, "annotationinfo", "name", Operator.EQUAL_TO, annotationMappings, false),
            null,
            new ColumnSpec(schemaName, "annotationinfo", "value"),
            ValueType.VALUE,
            null,
            jdbcTimestampPositionParser,
            null)
        };
        return ppSpecs;
    }

    @Override
    protected StagingSpec[] stagedSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        return new StagingSpec[0];
    }

    @Override
    public String getKeyType() {
        return "Patient";
    }

    @Override
    public String getKeyTypeDisplayName() {
        return "patient";
    }

    @Override
    public void close() throws BackendCloseException {
        boolean annotationMappingsClosed = false;
        try {
            super.close();
            this.annotationMappings.close();
            annotationMappingsClosed = true;
        } catch (IOException ex) {
            throw new DataSourceBackendCloseException(ex);
        } finally {
            if (!annotationMappingsClosed) {
                try {
                    this.annotationMappings.close();
                } catch (IOException ignore) {

                }
            }
        }
    }

}
