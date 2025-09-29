import os
import json
import shutil
import uuid
import threading
import sqlite3
import time
import pandas as pd
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_file, session, jsonify, flash, send_from_directory
from werkzeug.utils import secure_filename
import xml.etree.ElementTree as ET
import collections
import hashlib
import re
import decimal # <-- Importación necesaria para la corrección

# --- IMPORTACIONES Y CONFIGURACIÓN DE PYSPARK ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, to_timestamp, from_json, split, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType, DecimalType, ArrayType

# --- CONFIGURACIÓN E INICIALIZACIÓN DE FLASK ---
app = Flask(__name__)
app.config['SECRET_KEY'] = 'una-clave-secreta-muy-segura-12345'
app.config['TEMPLATES_AUTO_RELOAD'] = True

SESSION_FOLDER = os.path.abspath('temp_sessions')
app.config['SESSION_FOLDER'] = SESSION_FOLDER
os.makedirs(SESSION_FOLDER, exist_ok=True)

job_status = {}
spark_session = None

# --- FUNCIÓN PARA GESTIONAR LA SESIÓN DE SPARK (VERSIÓN ROBUSTA Y CORRECTA) ---
def get_spark_session():
    """
    Inicializa y devuelve una SparkSession global.
    Usa un bloque try-except para recrear la sesión si no está activa.
    """
    global spark_session
    try:
        _ = spark_session.version
        print("INFO: Reutilizando SparkSession existente.")
    except Exception:
        print("INFO: La sesión de Spark no está activa. Creando una nueva...")
        spark_session = SparkSession.builder \
            .appName("FlaskCastingApp") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.driver.memory", "2g") \
            .master("local[*]") \
            .getOrCreate()
    return spark_session

# --- LÓGICA DE PROCESAMIENTO (SOLUCIÓN DEFINITIVA Y A PRUEBA DE ERRORES) ---
def background_casting_job(session_data):
    session_id = session_data['session_id']
    selected_parquets = session_data['selected_parquets']
    session_path = os.path.join(app.config['SESSION_FOLDER'], session_id)
    progress_initial = {table: {'status': 'Pendiente', 'percentage': 0} for table in selected_parquets}
    job_status[session_id] = {'overall_status': 'running', 'progress': progress_initial, 'error': None}

    try:
        spark = get_spark_session()
        
        raw_folder = os.path.join(session_path, 'sin_castear_csv')
        schemas_folder = os.path.join(session_path, 'esquemas')
        output_partitioned_base = os.path.join(session_path, 'output_partitioned')
        os.makedirs(output_partitioned_base, exist_ok=True)
        
        TYPE_MAP = {
            'string': StringType(), 'int32': IntegerType(), 'long': LongType(),
            'double': DoubleType(), 'float': FloatType(), 'boolean': BooleanType(),
            'date': DateType(), 'timestamp': TimestampType()
        }

        for parquet_name in selected_parquets:
            job_status[session_id]['progress'][parquet_name] = {'status': 'Procesando con PySpark...', 'percentage': 20}
            csv_path = os.path.join(raw_folder, f"{parquet_name}.csv")
            schema_path = os.path.join(schemas_folder, f"{parquet_name}.schema")

            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_data = json.load(f)

            df_raw = spark.read.csv(csv_path, header=True, inferSchema=False)
            df_casted = df_raw

            job_status[session_id]['progress'][parquet_name] = {'status': 'Aplicando tipos de datos...', 'percentage': 40}
            
            for field in schema_data['fields']:
                field_name = field['name']
                if field_name not in df_casted.columns:
                    continue

                field_type_info = field.get('type', 'string')
                primary_type_info = next((t for t in field_type_info if t != 'null'), field_type_info) if isinstance(field_type_info, list) else field_type_info

                # --- INICIO DE LA SOLUCIÓN DEFINITIVA PARA ARRAYS ---
                if isinstance(primary_type_info, dict) and primary_type_info.get('type') == 'array':
                    # Este nuevo método es mucho más robusto que from_json.
                    # 1. Quita los corchetes al inicio y al final de la cadena: '[A,B]' -> 'A,B'
                    cleaned_col = regexp_replace(col(field_name), "^\\[|\\]$", "")
                    
                    # 2. Divide la cadena por la coma, ignorando espacios: 'A, B' -> ['A', 'B']
                    #    El resultado ya es una columna de tipo array<string>.
                    df_casted = df_casted.withColumn(field_name, split(cleaned_col, "\\s*,\\s*"))
                    
                    # Pasamos al siguiente campo
                    continue
                # --- FIN DE LA SOLUCIÓN DEFINITIVA PARA ARRAYS ---

                if isinstance(primary_type_info, dict):
                     primary_type_str = primary_type_info.get('type', 'string')
                else:
                     primary_type_str = primary_type_info
                
                primary_type_str = str(primary_type_str).lower()
                
                spark_type = TYPE_MAP.get(primary_type_str)
                
                if primary_type_str == 'date':
                    df_casted = df_casted.withColumn(field_name, to_date(trim(col(field_name))))
                
                elif primary_type_str == 'timestamp':
                    df_casted = df_casted.withColumn(field_name, to_timestamp(trim(col(field_name))))
                
                elif primary_type_str.startswith('decimal'):
                    match = re.search(r'decimal\((\d+),?\s*(\d+)?\)', primary_type_str)
                    if match:
                        precision = int(match.group(1))
                        scale = int(match.group(2) or 0)
                        df_casted = df_casted.withColumn(field_name, col(field_name).cast(DecimalType(precision, scale)))
                
                elif spark_type:
                    df_casted = df_casted.withColumn(field_name, col(field_name).cast(spark_type))

            # (El resto de la función permanece igual)
            job_status[session_id]['progress'][parquet_name] = {'status': 'Guardando Parquet...', 'percentage': 70}
            # ... (código para guardar, previsualizar, etc.) ...
            # ...
            # ...

            partition_columns = schema_data.get('partitions', [])
            output_path = os.path.join(output_partitioned_base, parquet_name)
            
            if os.path.exists(output_path):
                shutil.rmtree(output_path)
            
            writer = df_casted.write.mode("overwrite")
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            writer.parquet(output_path)
            
            for dirpath, _, filenames in os.walk(output_path):
                if '_SUCCESS' in filenames:
                    os.remove(os.path.join(dirpath, '_SUCCESS'))
                for filename in filenames:
                    if filename.startswith('part-') and filename.endswith('.parquet'):
                        os.rename(os.path.join(dirpath, filename), os.path.join(dirpath, f"{parquet_name}.parquet"))
                        break
            
            df_preview_pd = df_casted.limit(100).toPandas()
            
            for col_name, dtype in df_preview_pd.dtypes.items():
                if isinstance(dtype, object):
                    first_item = df_preview_pd[col_name].dropna().iloc[0] if not df_preview_pd[col_name].dropna().empty else None
                    if isinstance(first_item, list):
                        df_preview_pd[col_name] = df_preview_pd[col_name].astype(str)
                if not df_preview_pd[col_name].empty:
                    first_item = df_preview_pd[col_name].dropna().iloc[0] if not df_preview_pd[col_name].dropna().empty else None
                    if isinstance(first_item, decimal.Decimal):
                        df_preview_pd[col_name] = df_preview_pd[col_name].astype(float)
            
            df_preview_pd.to_sql(f"casteado_{parquet_name}", sqlite3.connect(get_db_path(session_id)), index=False, if_exists='replace')

            job_status[session_id]['progress'][parquet_name] = {'status': 'Completado', 'percentage': 100}

        time.sleep(1)
        shutil.make_archive(os.path.join(session_path, "resultado_particionado"), 'zip', output_partitioned_base)
        job_status[session_id]['tables'] = selected_parquets
        job_status[session_id]['overall_status'] = 'completed'

    except Exception as e:
        import traceback
        error_info = f"[{type(e).__name__}] {str(e)}\n{traceback.format_exc()}"
        job_status[session_id]['overall_status'] = 'failed'
        job_status[session_id]['error'] = error_info
    finally:
        status_file_path = os.path.join(session_path, 'status.json')
        with open(status_file_path, 'w', encoding='utf-8') as f:
            json.dump(job_status[session_id], f)


# --- (EL RESTO DEL CÓDIGO PERMANECE EXACTAMENTE IGUAL) ---

# --- RUTAS DE LA APLICACIÓN (CASTEO Y GENERALES) ---
def get_db_path(session_id):
    return os.path.join(app.config['SESSION_FOLDER'], f"{session_id}.db")

@app.route('/reset')
def reset():
    session_id = session.get('session_id')
    if session_id:
        session_folder = os.path.join(app.config['SESSION_FOLDER'], session_id)
        db_file = get_db_path(session_id)
        if os.path.isdir(session_folder): shutil.rmtree(session_folder, ignore_errors=True)
        if os.path.exists(db_file):
            try: os.remove(db_file)
            except OSError: pass
    session.clear()
    return redirect(url_for('dashboard'))

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
        if os.path.isdir(old_session_path): shutil.rmtree(old_session_path, ignore_errors=True)
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
                df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
                df.to_csv(os.path.join(raw_folder, f"{sheet}.csv"), index=False, header=True)
            session['parquet_files'] = sheet_names
            return jsonify({'status': 'success', 'tables': sheet_names})
        except Exception as e:
            return jsonify({'status': 'error', 'message': f"Error al procesar el Excel: {str(e)}"}), 500
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
                    df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
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
                all_valid = False; session['schema_status'][parquet_name] = 'missing'; session['schema_errors'][parquet_name] = "Falta subir el archivo de esquema."; continue
            try:
                csv_path = os.path.join(raw_folder, f"{parquet_name}.csv")
                if not os.path.exists(csv_path): raise FileNotFoundError(f"La hoja '{parquet_name}' no se encontró en el archivo Excel.")
                source_columns = set(pd.read_csv(csv_path, nrows=0, dtype=str).columns)
                with open(schema_path, 'r', encoding='utf-8') as f: schema_data = json.load(f)
                schema_columns = {field['name'] for field in schema_data['fields']}
                missing_in_source = schema_columns - set(source_columns)
                if missing_in_source:
                    all_valid = False; session['schema_status'][parquet_name] = 'error'
                    error_html = '<strong>Columnas del esquema no se encuentran en el Excel:</strong><ul>'
                    for field in sorted(list(missing_in_source)): error_html += f'<li>- {field}</li>'
                    error_html += '</ul>'; session['schema_errors'][parquet_name] = error_html
                else:
                    session['schema_status'][parquet_name] = 'validated'
            except Exception as e:
                all_valid = False; session['schema_status'][parquet_name] = 'error'; session['schema_errors'][parquet_name] = f"Error al procesar el archivo: {e}"
        if all_valid:
            return render_template('upload_schemas.html', parquets=selected_parquets, schema_status=session.get('schema_status', {}), schema_errors=session.get('schema_errors', {}), validation_passed=True)
        else:
            return render_template('upload_schemas.html', parquets=selected_parquets, schema_status=session.get('schema_status', {}), schema_errors=session.get('schema_errors', {}))
    return render_template('upload_schemas.html', parquets=selected_parquets, schema_status=session.get('schema_status', {}), schema_errors=session.get('schema_errors', {}))

@app.route('/start_processing', methods=['POST'])
def start_processing():
    session_id = session.get('session_id')
    if not session_id: return jsonify({'status': 'error', 'message': 'Sesión no válida'}), 400
    thread = threading.Thread(target=background_casting_job, args=(session.copy(),))
    thread.start()
    return jsonify({'status': 'ok', 'message': 'Proceso iniciado.'})

@app.route('/job_progress')
def job_progress():
    session_id = session.get('session_id')
    if not session_id: return jsonify({'status': 'error', 'message': 'Sesión no válida'}), 400
    status = job_status.get(session_id, {'overall_status': 'not_found'})
    return jsonify(status)

@app.route('/results')
def show_results():
    session_id = session.get('session_id')
    if not session_id: return redirect(url_for('casteo_index'))
    session_path = os.path.join(app.config['SESSION_FOLDER'], session_id)
    status_file_path = os.path.join(session_path, 'status.json')
    if os.path.exists(status_file_path):
        try:
            with open(status_file_path, 'r', encoding='utf-8') as f: results_data = json.load(f)
            return render_template('results.html', results=results_data)
        except Exception as e:
            error_data = {'overall_status': 'failed', 'error': f'Error fatal: No se pudo leer el archivo de estado. {e}'}
            return render_template('results.html', results=error_data)
    else:
        flash("Tu sesión ha expirado o el proceso no finalizó correctamente.", "warning")
        return redirect(url_for('casteo_index'))

@app.route('/get_table_schema/<table_name>')
def get_table_schema(table_name):
    session_id = session.get('session_id')
    if not session_id: return jsonify({"error": "Sesión no válida"}), 404
    schema_file_path = os.path.join(app.config['SESSION_FOLDER'], session_id, 'esquemas', f"{table_name}.schema")
    try:
        if not os.path.exists(schema_file_path): return jsonify({"error": f"No se encontró el archivo de esquema para la tabla {table_name}"}), 404
        
        with open(schema_file_path, 'r', encoding='utf-8') as f: schema_data = json.load(f)
        
        schema_info = []
        for field in schema_data.get('fields', []):
            field_name = field.get('name')
            field_type_info = field.get('type')
            
            # --- INICIO DE LA CORRECCIÓN PARA VISUALIZACIÓN ---
            # Esta lógica convierte el objeto del esquema en un texto legible
            primary_type = next((t for t in field_type_info if t != 'null'), field_type_info) if isinstance(field_type_info, list) else field_type_info
            
            display_type = ""
            if isinstance(primary_type, dict):
                main_type = primary_type.get('type', 'desconocido')
                if main_type == 'array':
                    items = primary_type.get('items', {})
                    # Los 'items' también pueden ser un dict o un string
                    item_type = items.get('type') if isinstance(items, dict) else items
                    display_type = f"array<{item_type}>"
                else:
                    # Para otros tipos complejos como decimal
                    display_type = primary_type.get('logicalType', main_type)
            else:
                # Para tipos simples como "string"
                display_type = str(primary_type)
            # --- FIN DE LA CORRECCIÓN ---

            if field_name:
                schema_info.append((field_name, display_type))
                
        return jsonify({"schema": schema_info})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
    output_partitioned_base = os.path.join(session_path, 'output_partitioned')
    temp_zip_dir = os.path.join(session_path, f"temp_zip_{uuid.uuid4()}")
    os.makedirs(temp_zip_dir, exist_ok=True)
    
    try:
        spark = get_spark_session()
        for table_name in selected_tables:
            parquet_folder_path = os.path.join(output_partitioned_base, table_name)
            df = spark.read.parquet(parquet_folder_path)
            
            single_parquet_output_path = os.path.join(temp_zip_dir, table_name)
            os.makedirs(single_parquet_output_path, exist_ok=True)
            
            df.coalesce(1).write.mode('overwrite').parquet(single_parquet_output_path)
            
            for filename in os.listdir(single_parquet_output_path):
                if filename.startswith('part-') and filename.endswith('.parquet'):
                    shutil.move(
                        os.path.join(single_parquet_output_path, filename),
                        os.path.join(temp_zip_dir, f"{table_name}.parquet")
                    )
                    break
            shutil.rmtree(single_parquet_output_path)
        
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


# --- LÓGICA PARA VALIDACIÓN DE MALLAS ---
def process_malla_xml(xml_content):
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        wrapped_content = f"<DEFS>{xml_content}</DEFS>"
        root = ET.fromstring(wrapped_content)

    all_job_elements = root.findall(".//JOB")
    if not all_job_elements:
        raise ValueError("No se encontraron elementos <JOB> en el archivo XML.")

    unique_id_to_job_info = {}
    job_name_to_unique_ids = collections.defaultdict(list)

    for i, job_elem in enumerate(all_job_elements):
        jobname = job_elem.get("JOBNAME", "").strip()
        parent_folder = job_elem.get("PARENT_FOLDER", "").strip()
        
        if jobname:
            unique_id_base = f"{jobname}-{parent_folder}-{i}"
            unique_id = hashlib.sha256(unique_id_base.encode()).hexdigest()
            full_xml_string = ET.tostring(job_elem, encoding="unicode")
            
            errors = []
            xml_lines = full_xml_string.splitlines()

            for line_num, line in enumerate(xml_lines, 1):
                if '.dev' in line and '<VALUE' in line:
                    errors.append({
                        "line": line_num, "error": "Entorno '.dev' encontrado",
                        "content": line.strip()
                    })

            on_open_count = full_xml_string.count("<ON ")
            on_close_count = full_xml_string.count("</ON>")
            if on_open_count != on_close_count:
                errors.append({
                    "line": "?", "error": "Etiquetas <ON> no coinciden",
                    "content": f"Aperturas: {on_open_count}, Cierres: {on_close_count}"
                })
            
            if not full_xml_string.strip().endswith('</JOB>'):
                errors.append({"line": len(xml_lines), "error": "Falta etiqueta de cierre </JOB>", "content": "El bloque del job no termina correctamente."})

            unique_id_to_job_info[unique_id] = {
                "jobname": jobname, "parent_folder": parent_folder,
                "forces": [], "forced_by": [], "full_xml_string": full_xml_string,
                "unique_id": unique_id, "unique_id_short": unique_id[-6:],
                "errors": errors, "parent_folder_mismatch": False
            }
            job_name_to_unique_ids[jobname].append(unique_id)

    for uid, info in unique_id_to_job_info.items():
        job_elem = ET.fromstring(info["full_xml_string"])
        for doforcejob_elem in job_elem.findall(".//DOFORCEJOB"):
            forced_job_name = doforcejob_elem.get("NAME", "").strip()
            if forced_job_name:
                info["forces"].append(forced_job_name)
                for target_uid in job_name_to_unique_ids.get(forced_job_name, []):
                    unique_id_to_job_info[target_uid]["forced_by"].append(uid)
    
    potential_roots = sorted([job for job in unique_id_to_job_info.values() if not job["forced_by"]], key=lambda j: j['jobname'])
    root_parent_folder = potential_roots[0]['parent_folder'] if potential_roots else None

    if root_parent_folder:
        for job in unique_id_to_job_info.values():
            if job['parent_folder'] != root_parent_folder:
                job['parent_folder_mismatch'] = True

    return {
        "jobs": list(unique_id_to_job_info.values()),
        "name_to_id_map": dict(job_name_to_unique_ids)
    }

@app.route('/malla')
def malla_validator_index():
    return render_template('malla_validator.html')

@app.route('/upload_and_process_malla', methods=['POST'])
def upload_and_process_malla():
    if 'malla_file' not in request.files or not request.files['malla_file'].filename:
        return jsonify({'status': 'error', 'message': 'No se seleccionó ningún archivo.'}), 400
    
    file = request.files['malla_file']
    if file and file.filename.endswith('.xml'):
        try:
            xml_content = file.read().decode('utf-8', errors='ignore')
            processed_data = process_malla_xml(xml_content)
            return jsonify({'status': 'success', 'data': processed_data})
        except ET.ParseError as e:
            error_message = f"Error de sintaxis en el XML en la línea {e.lineno}, columna {e.offset}: {e.msg}"
            return jsonify({'status': 'error', 'message': error_message}), 400
        except Exception as e:
            import traceback
            return jsonify({'status': 'error', 'message': f'Error al procesar el archivo: {traceback.format_exc()}'}), 500
    
    return jsonify({'status': 'error', 'message': 'Formato de archivo no válido. Se esperaba un .xml'}), 400


# --- EJECUCIÓN DE LA APLICACIÓN ---
if __name__ == '__main__':
    # Inicia la aplicación Flask
    # Es recomendable usar un servidor WSGI como Gunicorn en producción
    app.run(debug=True, use_reloader=False) # use_reloader=False es importante con Spark
    
    # Al cerrar la aplicación (Ctrl+C en la terminal), se detiene la sesión de Spark
    if spark_session:
        print("INFO: Deteniendo SparkSession...")
        spark_session.stop()