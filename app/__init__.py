import os
import json
import shutil
import uuid
import threading
import sqlite3
import io
import time
from flask import Flask, render_template, request, redirect, url_for, send_file, session, jsonify, flash, send_from_directory
from werkzeug.utils import secure_filename
from pyspark.sql import SparkSession
import pandas as pd
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lpad, to_timestamp, to_date

# --- CONFIGURACIÓN E INICIALIZACIÓN ---
app = Flask(__name__)
app.config['SECRET_KEY'] = 'una-clave-secreta-muy-segura-12345'
app.config['TEMPLATES_AUTO_RELOAD'] = True

SESSION_FOLDER = os.path.abspath('temp_sessions')
SPARK_TEMP_DIR = os.path.abspath('spark_temp')

app.config['SESSION_FOLDER'] = SESSION_FOLDER
os.makedirs(SESSION_FOLDER, exist_ok=True)
os.makedirs(SPARK_TEMP_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("WebApp Final") \
    .master("local[*]") \
    .config("driver-memory", "2g") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.local.dir", SPARK_TEMP_DIR) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .getOrCreate()

job_status = {}

# --- LÓGICA DE PROCESAMIENTO ---
def background_casting_job(session_data):
    session_id = session_data['session_id']
    selected_parquets = session_data['selected_parquets']
    session_path = os.path.join(app.config['SESSION_FOLDER'], session_id)
    progress_initial = {table: {'status': 'Pendiente', 'percentage': 0} for table in selected_parquets}
    job_status[session_id] = {'overall_status': 'running', 'progress': progress_initial, 'error': None}
    
    try:
        raw_folder = os.path.join(session_path, 'sin_castear_csv')
        schemas_folder = os.path.join(session_path, 'esquemas')
        output_partitioned_base = os.path.join(session_path, 'output_partitioned')
        os.makedirs(output_partitioned_base, exist_ok=True)
        conn = sqlite3.connect(get_db_path(session_id))
        TYPE_MAP = {'int32': 'integer', 'long': 'long', 'string': 'string','timestamp': 'timestamp', 'date': 'date', 'double': 'double','float': 'float', 'boolean': 'boolean'}

        for parquet_name in selected_parquets:
            job_status[session_id]['progress'][parquet_name] = {'status': 'Casteando tipos...', 'percentage': 33}
            df_raw = spark.read.option("header", "true").option("inferSchema", "false").csv(os.path.join(raw_folder, f"{parquet_name}.csv"))
            with open(os.path.join(schemas_folder, f"{parquet_name}.schema"), 'r') as f: schema_data = json.load(f)
            partition_columns = schema_data.get('partitions', [])
            df_casted = df_raw
            for field in schema_data['fields']:
                field_name = field['name']
                if field_name not in df_casted.columns: continue
                field_type_info = field['type']
                primary_type = [t for t in field_type_info if t != 'null'][0] if isinstance(field_type_info, list) else field_type_info
                spark_type_str = TYPE_MAP.get(primary_type.lower(), primary_type)
                if 'format' in field and primary_type.lower() in ['timestamp', 'date']:
                    func = to_timestamp if primary_type.lower() == 'timestamp' else to_date
                    df_casted = df_casted.withColumn(field_name, func(col(field_name), field['format']))
                else:
                    df_casted = df_casted.withColumn(field_name, col(field_name).cast(spark_type_str))
            potential_padding_columns = ['partition_data_month_id', 'partition_data_day_id']
            for column_name in partition_columns:
                if column_name in df_casted.columns and column_name in potential_padding_columns:
                    df_casted = df_casted.withColumn(column_name, lpad(col(column_name), 2, '0'))
            job_status[session_id]['progress'][parquet_name] = {'status': 'Guardando archivos...', 'percentage': 66}
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
            result_pandas_df = df_casted.toPandas()
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            for col_name in result_pandas_df.columns:
                if result_pandas_df[col_name].dtype == 'object':
                    is_decimal_col = result_pandas_df[col_name].dropna().apply(lambda x: isinstance(x, Decimal)).any()
                    if is_decimal_col:
                        result_pandas_df[col_name] = result_pandas_df[col_name].apply(lambda x: float(x) if x is not None else None)
            result_pandas_df.to_sql(f"casteado_{parquet_name}", conn, index=False, if_exists='replace')
            output_partitioned_path = os.path.join(output_partitioned_base, parquet_name)
            writer_part = df_casted.coalesce(1).write.mode('overwrite')
            if partition_columns: writer_part = writer_part.partitionBy(*partition_columns)
            writer_part.parquet(output_partitioned_path)
            for dirpath, _, filenames in os.walk(output_partitioned_path):
                if '_SUCCESS' in filenames: os.remove(os.path.join(dirpath, '_SUCCESS'))
                for filename in filenames:
                    if filename.startswith('part-'):
                        os.rename(os.path.join(dirpath, filename), os.path.join(dirpath, f"{parquet_name}.parquet"))
                        break
            job_status[session_id]['progress'][parquet_name] = {'status': 'Completado', 'percentage': 100}
        
        conn.close()
        
        for table in selected_parquets:
            job_status[session_id]['progress'][table] = {'status': 'Completado', 'percentage': 100}
        time.sleep(1) 
        
        shutil.make_archive(os.path.join(session_path, "resultado_particionado"), 'zip', output_partitioned_base)
        
        job_status[session_id]['tables'] = selected_parquets
        job_status[session_id]['overall_status'] = 'completed'
        
        status_file_path = os.path.join(session_path, 'status.json')
        with open(status_file_path, 'w') as f:
            json.dump(job_status[session_id], f)

    except Exception as e:
        import traceback
        error_info = f"[{type(e).__name__}] {str(e)}\n{traceback.format_exc()}"
        job_status[session_id]['overall_status'] = 'failed'
        job_status[session_id]['error'] = error_info
        status_file_path = os.path.join(session_path, 'status.json')
        with open(status_file_path, 'w') as f:
            json.dump(job_status[session_id], f)

# --- RUTAS DE LA APLICACIÓN ---
def get_db_path(session_id):
    return os.path.join(app.config['SESSION_FOLDER'], f"{session_id}.db")

@app.route('/reset')
def reset():
    session_id = session.get('session_id')
    if session_id:
        session_folder = os.path.join(app.config['SESSION_FOLDER'], session_id)
        db_file = get_db_path(session_id)
        shutil.rmtree(session_folder, ignore_errors=True)
        if os.path.exists(db_file):
            try: os.remove(db_file)
            except OSError: pass
    session.clear()
    return redirect(url_for('casteo_index'))

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/casteo')
def casteo_index():
    return render_template('casteo_index.html')

@app.route('/upload_and_process_excel', methods=['POST'])
def upload_and_process_excel():
    session_id = session.get('session_id')
    if session_id:
        old_session_path = os.path.join(app.config['SESSION_FOLDER'], session_id)
        old_db_path = get_db_path(session_id)
        shutil.rmtree(old_session_path, ignore_errors=True)
        if os.path.exists(old_db_path):
            try: os.remove(old_db_path)
            except OSError: pass
    session.clear()

    if 'excel_file' not in request.files or not request.files['excel_file'].filename:
        return jsonify({'status': 'error', 'message': 'No se seleccionó ningún archivo.'}), 400
    
    file = request.files['excel_file']
    if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        session_path = os.path.join(app.config['SESSION_FOLDER'], session_id)
        raw_folder = os.path.join(session_path, 'sin_castear_csv')
        os.makedirs(raw_folder, exist_ok=True)
        try:
            xls = pd.ExcelFile(file.stream.read())
            sheet_names = list(xls.sheet_names)
            for sheet in sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet).astype(str)
                df.to_csv(os.path.join(raw_folder, f"{sheet}.csv"), index=False, header=True)
            session['parquet_files'] = sheet_names
            return jsonify({'status': 'success', 'tables': sheet_names})
        except Exception as e:
            return jsonify({'status': 'error', 'message': f"Error al procesar el Excel: {e}"}), 500
    
    return jsonify({'status': 'error', 'message': 'Formato de archivo no válido.'}), 400

@app.route('/select_parquets', methods=['POST'])
def select_parquets():
    if 'parquet_files' not in session: return redirect(url_for('casteo_index'))
    session['selected_parquets'] = request.form.getlist('parquet_checkbox')
    if not session['selected_parquets']:
        flash("Debes seleccionar al menos una tabla.", "warning")
        return redirect(request.referrer or url_for('casteo_index'))
    return redirect(url_for('upload_schemas'))

@app.route('/upload_schemas', methods=['GET', 'POST'])
def upload_schemas():
    if 'selected_parquets' not in session: return redirect(url_for('casteo_index'))
    session_id = session.get('session_id')
    selected_parquets = session['selected_parquets']
    session_path = os.path.join(app.config['SESSION_FOLDER'], session_id)
    schemas_folder = os.path.join(session_path, 'esquemas')
    os.makedirs(schemas_folder, exist_ok=True)
    if 'schema_status' not in session: session['schema_status'] = {}
    if 'schema_errors' not in session: session['schema_errors'] = {}
    if request.method == 'POST':
        if 'new_excel_file' in request.files and request.files['new_excel_file'].filename:
            file = request.files['new_excel_file']
            if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
                raw_folder = os.path.join(session_path, 'sin_castear_csv')
                shutil.rmtree(raw_folder, ignore_errors=True)
                os.makedirs(raw_folder, exist_ok=True)
                xls = pd.ExcelFile(file.stream.read())
                for sheet in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet).astype(str)
                    df.to_csv(os.path.join(raw_folder, f"{sheet}.csv"), index=False, header=True)
                session['schema_status'] = {}
        for parquet_name in selected_parquets:
            form_field_name = f"schema_for_{parquet_name}"
            if form_field_name in request.files and request.files[form_field_name].filename:
                file = request.files[form_field_name]
                file.save(os.path.join(schemas_folder, f"{parquet_name}.schema"))
                session['schema_status'][parquet_name] = 'uploaded'
        all_valid = True
        session['schema_errors'] = {}
        raw_folder = os.path.join(session_path, 'sin_castear_csv')
        for parquet_name in selected_parquets:
            schema_path = os.path.join(schemas_folder, f"{parquet_name}.schema")
            if not os.path.exists(schema_path):
                all_valid = False
                session['schema_status'][parquet_name] = 'missing'
                session['schema_errors'][parquet_name] = "Falta subir el archivo de esquema."
                continue
            try:
                csv_path = os.path.join(raw_folder, f"{parquet_name}.csv")
                if not os.path.exists(csv_path):
                     raise FileNotFoundError(f"La hoja '{parquet_name}' no se encontró en el archivo Excel.")
                source_columns = set(pd.read_csv(csv_path, nrows=0).columns)
                with open(schema_path, 'r') as f: schema_data = json.load(f)
                schema_columns = {field['name'] for field in schema_data['fields']}
                missing_in_source = schema_columns - source_columns
                if missing_in_source:
                    all_valid = False
                    session['schema_status'][parquet_name] = 'error'
                    error_html = '<strong>Error:...</strong><ul>'
                    for field in sorted(list(missing_in_source)):
                        error_html += f'<li>- {field}</li>'
                    error_html += '</ul>'
                    session['schema_errors'][parquet_name] = error_html
                else:
                    session['schema_status'][parquet_name] = 'validated'
            except Exception as e:
                all_valid = False
                session['schema_status'][parquet_name] = 'error'
                session['schema_errors'][parquet_name] = f"Error al procesar el archivo: {e}"
        if all_valid:
            return render_template(
                'upload_schemas.html',
                parquets=selected_parquets,
                schema_status=session.get('schema_status', {}),
                schema_errors=session.get('schema_errors', {}),
                validation_passed=True
            )
        else:
            return redirect(url_for('upload_schemas'))
    return render_template(
        'upload_schemas.html',
        parquets=selected_parquets,
        schema_status=session.get('schema_status', {}),
        schema_errors=session.get('schema_errors', {})
    )
@app.route('/start_processing', methods=['POST'])
def start_processing():
    session_id = session.get('session_id')
    if not session_id:
        return jsonify({'status': 'error', 'message': 'Sesión no válida'}), 400
    thread = threading.Thread(target=background_casting_job, args=(session.copy(),))
    thread.start()
    return jsonify({'status': 'ok', 'message': 'Proceso iniciado.'})
@app.route('/job_progress')
def job_progress():
    session_id = session.get('session_id')
    if not session_id:
        return jsonify({'status': 'error', 'message': 'Sesión no válida'}), 400
    status = job_status.get(session_id, {'overall_status': 'not_found'})
    return jsonify(status)
@app.route('/results')
def show_results():
    session_id = session.get('session_id')
    if not session_id:
        return redirect(url_for('casteo_index'))
    session_path = os.path.join(app.config['SESSION_FOLDER'], session_id)
    status_file_path = os.path.join(session_path, 'status.json')
    if os.path.exists(status_file_path):
        try:
            with open(status_file_path, 'r') as f:
                results_data = json.load(f)
            return render_template('results.html', results=results_data)
        except Exception as e:
            error_data = {'overall_status': 'failed', 'error': f'Error fatal: No se pudo leer el archivo de estado. {e}'}
            return render_template('results.html', results=error_data)
    else:
        flash("Tu sesión ha expirado o el proceso no finalizó correctamente.", "warning")
        return redirect(url_for('casteo_index'))
@app.route('/get_raw_data_preview/<table_name>')
def get_raw_data_preview(table_name):
    session_id = session.get('session_id')
    if not session_id:
        return jsonify({"error": "Sesión no válida"}), 404
    raw_csv_path = os.path.join(app.config['SESSION_FOLDER'], session_id, 'sin_castear_csv', f"{table_name}.csv")
    try:
        if not os.path.exists(raw_csv_path):
            return jsonify({"error": f"No se encontró el archivo de datos crudos para la tabla {table_name}"}), 404
        df = pd.read_csv(raw_csv_path, dtype=str).head(10)
        return jsonify({"html": df.to_html(classes='table table-bordered table-sm', index=False)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/get_table_schema/<table_name>')
def get_table_schema(table_name):
    session_id = session.get('session_id')
    if not session_id: return jsonify({"error": "Sesión no válida"}), 404
    schema_file_path = os.path.join(app.config['SESSION_FOLDER'], session_id, 'esquemas', f"{table_name}.schema")
    try:
        if not os.path.exists(schema_file_path): return jsonify({"error": f"No se encontró el archivo de esquema para la tabla {table_name}"}), 404
        with open(schema_file_path, 'r') as f: schema_data = json.load(f)
        schema_info = []
        for field in schema_data.get('fields', []):
            field_name, field_type_info = field.get('name'), field.get('type')
            primary_type = next((t for t in field_type_info if t != 'null'), 'desconocido') if isinstance(field_type_info, list) else field_type_info
            if field_name: schema_info.append((field_name, primary_type))
        return jsonify({"schema": schema_info})
    except Exception as e: return jsonify({"error": str(e)}), 500
@app.route('/get_table_preview/<table_name>')
def get_table_preview(table_name):
    session_id = session.get('session_id')
    if not session_id: return jsonify({"error": "Sesión no válida"}), 404
    db_path = get_db_path(session_id)
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(f'SELECT * FROM "casteado_{table_name}" LIMIT 10', conn)
        conn.close()
        return jsonify({"html": df.to_html(classes='table table-bordered table-sm', index=False)})
    except Exception as e: return jsonify({"error": str(e)}), 500
@app.route('/download_selected_parquets', methods=['POST'])
def download_selected_parquets():
    session_id = session.get('session_id')
    if not session_id: return "Error: Sesión no encontrada.", 404
    selected_tables = request.form.getlist('selected_tables')
    if not selected_tables: return "Error: No se seleccionó ninguna tabla para descargar.", 400
    session_path = os.path.join(app.config['SESSION_FOLDER'], session_id)
    db_path = get_db_path(session_id)
    temp_zip_dir = os.path.join(session_path, f"temp_zip_{uuid.uuid4()}")
    os.makedirs(temp_zip_dir, exist_ok=True)
    try:
        conn = sqlite3.connect(db_path)
        for table_name in selected_tables:
            df = pd.read_sql_query(f'SELECT * FROM "casteado_{table_name}"', conn)
            parquet_file_path = os.path.join(temp_zip_dir, f'{table_name}.parquet')
            df.to_parquet(parquet_file_path, index=False)
        conn.close()
        zip_output_path_base = os.path.join(session_path, "parquets_consolidados")
        shutil.make_archive(zip_output_path_base, 'zip', temp_zip_dir)
        zip_full_path = f"{zip_output_path_base}.zip"
        return send_file(zip_full_path, as_attachment=True, download_name='parquets_consolidados.zip', mimetype='application/zip')
    finally:
        shutil.rmtree(temp_zip_dir, ignore_errors=True)
@app.route('/download/partitioned_zip')
def download_partitioned_zip():
    session_id = session.get('session_id')
    if not session_id: return "Error: Sesión no encontrada.", 404
    directory = os.path.join(app.config['SESSION_FOLDER'], session_id)
    return send_from_directory(directory, "resultado_particionado.zip", as_attachment=True)