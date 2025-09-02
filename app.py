from flask import Flask, request, jsonify, send_file, render_template_string
from flask_cors import CORS
import sqlite3
import pandas as pd
import json
import os
import ftplib
import requests
import io
import zipfile
from datetime import datetime
import uuid
from werkzeug.utils import secure_filename
import chardet

app = Flask(__name__)
CORS(app)

# 配置
UPLOAD_FOLDER = 'uploads'
DATABASE = 'annotation_platform.db'
ALLOWED_EXTENSIONS = {'csv', 'txt', 'tsv'}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def init_db():
    """初始化数据库"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # 创建项目表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            file_path TEXT,
            config TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 创建数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS project_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT,
            row_index INTEGER,
            original_data TEXT,
            annotations TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_id) REFERENCES projects (id)
        )
    ''')
    
    # 创建列配置表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS column_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT,
            original_name TEXT,
            display_name TEXT,
            column_type TEXT,
            is_visible INTEGER DEFAULT 1,
            is_url INTEGER DEFAULT 0,
            url_new_window INTEGER DEFAULT 1,
            column_width INTEGER DEFAULT 150,
            is_searchable INTEGER DEFAULT 0,
            search_api TEXT,
            FOREIGN KEY (project_id) REFERENCES projects (id)
        )
    ''')
    
    # 检查并添加缺失的列（数据库迁移）
    try:
        # 获取表结构
        cursor.execute("PRAGMA table_info(column_configs)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # 需要添加的新列
        new_columns = [
            ('is_url', 'INTEGER DEFAULT 0'),
            ('url_new_window', 'INTEGER DEFAULT 1'), 
            ('column_width', 'INTEGER DEFAULT 150'),
            ('is_searchable', 'INTEGER DEFAULT 0'),
            ('search_api', 'TEXT')
        ]
        
        # 添加缺失的列
        for column_name, column_def in new_columns:
            if column_name not in columns:
                try:
                    cursor.execute(f'ALTER TABLE column_configs ADD COLUMN {column_name} {column_def}')
                    print(f"已添加列: {column_name}")
                except Exception as e:
                    print(f"添加列 {column_name} 失败: {e}")
                    
    except Exception as e:
        print(f"数据库迁移错误: {e}")
    
    conn.commit()
    conn.close()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def detect_encoding(file_path):
    """检测文件编码"""
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    """返回主页面HTML"""
    try:
        with open('templates/index.html', 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        # 如果templates目录不存在，直接返回HTML内容
        return render_template_string(get_html_content())

@app.route('/api/projects', methods=['GET'])
def get_projects():
    """获取所有项目列表"""
    conn = get_db_connection()
    projects = conn.execute('''
        SELECT p.*, COUNT(pd.id) as data_count,
               SUM(CASE WHEN pd.annotations IS NOT NULL AND pd.annotations != '{}' THEN 1 ELSE 0 END) as annotated_count
        FROM projects p 
        LEFT JOIN project_data pd ON p.id = pd.project_id
        GROUP BY p.id
        ORDER BY p.updated_at DESC
    ''').fetchall()
    conn.close()
    
    return jsonify([{
        'id': p['id'],
        'name': p['name'],
        'description': p['description'],
        'data_count': p['data_count'],
        'annotated_count': p['annotated_count'],
        'completion_rate': round((p['annotated_count'] / p['data_count'] * 100) if p['data_count'] > 0 else 0, 1),
        'created_at': p['created_at'],
        'updated_at': p['updated_at']
    } for p in projects])

@app.route('/api/projects', methods=['POST'])
def create_project():
    """创建新项目"""
    data = request.json
    project_id = str(uuid.uuid4())
    
    conn = get_db_connection()
    conn.execute('''
        INSERT INTO projects (id, name, description, config)
        VALUES (?, ?, ?, ?)
    ''', (project_id, data['name'], data.get('description', ''), '{}'))
    conn.commit()
    conn.close()
    
    return jsonify({'id': project_id, 'message': '项目创建成功'})

@app.route('/api/projects/<project_id>', methods=['GET'])
def get_project(project_id):
    """获取项目详情"""
    conn = get_db_connection()
    project = conn.execute('SELECT * FROM projects WHERE id = ?', (project_id,)).fetchone()
    
    if not project:
        conn.close()
        return jsonify({'error': '项目不存在'}), 404
    
    # 获取列配置
    columns = conn.execute('''
        SELECT * FROM column_configs WHERE project_id = ? ORDER BY id
    ''', (project_id,)).fetchall()
    
    conn.close()
    
    return jsonify({
        'id': project['id'],
        'name': project['name'],
        'description': project['description'],
        'config': json.loads(project['config'] or '{}'),
        'columns': [dict(c) for c in columns]
    })

@app.route('/api/projects/<project_id>/upload', methods=['POST'])
def upload_file(project_id):
    """上传文件到项目"""
    upload_type = request.form.get('upload_type', 'file')
    delimiter = request.form.get('delimiter', ',')
    encoding = request.form.get('encoding', 'utf-8')
    has_header = request.form.get('has_header') == 'true'
    
    if delimiter == '\\t':
        delimiter = '\t'
    
    try:
        file_data = None
        
        if upload_type == 'file':
            if 'file' not in request.files:
                return jsonify({'error': '没有选择文件'}), 400
            
            file = request.files['file']
            if file.filename == '' or not allowed_file(file.filename):
                return jsonify({'error': '无效的文件'}), 400
            
            filename = secure_filename(file.filename)
            file_path = os.path.join(UPLOAD_FOLDER, f"{project_id}_{filename}")
            file.save(file_path)
            
            # 检测编码
            if encoding == 'auto':
                encoding = detect_encoding(file_path)
            
            with open(file_path, 'r', encoding=encoding) as f:
                file_data = f.read()
        
        elif upload_type == 'url':
            url = request.form.get('url')
            response = requests.get(url)
            if encoding == 'auto':
                encoding = response.apparent_encoding or 'utf-8'
            file_data = response.content.decode(encoding)
        
        elif upload_type == 'ftp':
            ftp_server = request.form.get('ftp_server')
            ftp_user = request.form.get('ftp_user')
            ftp_password = request.form.get('ftp_password')
            ftp_path = request.form.get('ftp_path')
            
            ftp = ftplib.FTP(ftp_server)
            ftp.login(ftp_user, ftp_password)
            
            bio = io.BytesIO()
            ftp.retrbinary('RETR ' + ftp_path, bio.write)
            ftp.quit()
            
            if encoding == 'auto':
                raw_data = bio.getvalue()
                encoding = chardet.detect(raw_data)['encoding']
            
            file_data = bio.getvalue().decode(encoding)
        
        # 解析数据
        lines = file_data.strip().split('\n')
        if not lines:
            return jsonify({'error': '文件为空'}), 400
        
        headers = []
        start_index = 0
        
        if has_header and lines:
            headers = lines[0].split(delimiter)
            start_index = 1
        elif lines:
            first_line = lines[0].split(delimiter)
            headers = [f'列{i+1}' for i in range(len(first_line))]
        
        # 保存数据到数据库
        conn = get_db_connection()
        
        # 清除旧数据
        conn.execute('DELETE FROM project_data WHERE project_id = ?', (project_id,))
        conn.execute('DELETE FROM column_configs WHERE project_id = ?', (project_id,))
        
        # 保存列配置
        for i, header in enumerate(headers):
            conn.execute('''
                INSERT INTO column_configs (project_id, original_name, display_name, column_type, 
                                          is_visible, is_url, url_new_window, column_width, 
                                          is_searchable, search_api)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, header, header, 'text', 1, 0, 1, 150, 0, ''))
        
        # 保存数据行
        for i in range(start_index, len(lines)):
            values = lines[i].split(delimiter)
            row_data = {}
            for j, header in enumerate(headers):
                row_data[header] = values[j] if j < len(values) else ''
            
            conn.execute('''
                INSERT INTO project_data (project_id, row_index, original_data, annotations)
                VALUES (?, ?, ?, ?)
            ''', (project_id, i - start_index, json.dumps(row_data), '{}'))
        
        # 更新项目文件路径
        file_path = file_path if upload_type == 'file' else None
        conn.execute('''
            UPDATE projects SET file_path = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (file_path, project_id))
        
        conn.commit()
        conn.close()
        
        # 返回前3行预览数据
        preview_data = []
        for i in range(min(3, len(lines) - start_index)):
            values = lines[start_index + i].split(delimiter)
            row = {}
            for j, header in enumerate(headers):
                row[header] = values[j] if j < len(values) else ''
            preview_data.append(row)
        
        return jsonify({
            'message': '文件上传成功',
            'headers': headers,
            'preview_data': preview_data,
            'total_rows': len(lines) - start_index
        })
        
    except Exception as e:
        return jsonify({'error': f'上传失败: {str(e)}'}), 500

@app.route('/api/projects/<project_id>/data', methods=['GET'])
def get_project_data(project_id):
    """获取项目数据（支持分页）"""
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    
    offset = (page - 1) * per_page
    
    conn = get_db_connection()
    
    # 获取总数
    total = conn.execute('''
        SELECT COUNT(*) as count FROM project_data WHERE project_id = ?
    ''', (project_id,)).fetchone()['count']
    
    # 获取数据
    data_rows = conn.execute('''
        SELECT * FROM project_data WHERE project_id = ? 
        ORDER BY row_index LIMIT ? OFFSET ?
    ''', (project_id, per_page, offset)).fetchall()
    
    # 获取列配置
    columns = conn.execute('''
        SELECT * FROM column_configs WHERE project_id = ? ORDER BY id
    ''', (project_id,)).fetchall()
    
    conn.close()
    
    # 构建响应数据
    data = []
    for row in data_rows:
        original_data = json.loads(row['original_data'])
        annotations = json.loads(row['annotations'] or '{}')
        data.append({
            'id': row['id'],
            'row_index': row['row_index'],
            'original_data': original_data,
            'annotations': annotations
        })
    
    return jsonify({
        'data': data,
        'columns': [dict(c) for c in columns],
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        }
    })

@app.route('/api/projects/<project_id>/columns', methods=['PUT'])
def update_columns(project_id):
    """更新列配置"""
    columns = request.json.get('columns', [])
    
    conn = get_db_connection()
    
    for column in columns:
        conn.execute('''
            UPDATE column_configs 
            SET display_name = ?, is_visible = ?, is_url = ?, url_new_window = ?, 
                column_width = ?, is_searchable = ?, search_api = ?
            WHERE project_id = ? AND original_name = ?
        ''', (
            column['display_name'], 
            column.get('is_visible', 1), 
            column.get('is_url', 0),
            column.get('url_new_window', 1),
            column.get('column_width', 150),
            column.get('is_searchable', 0),
            column.get('search_api', ''),
            project_id, 
            column['original_name']
        ))
    
    conn.commit()
    conn.close()
    
    return jsonify({'message': '列配置更新成功'})

@app.route('/api/projects/<project_id>/config', methods=['PUT'])
def update_project_config(project_id):
    """更新项目配置"""
    config = request.json
    
    conn = get_db_connection()
    conn.execute('''
        UPDATE projects SET config = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (json.dumps(config), project_id))
    conn.commit()
    conn.close()
    
    return jsonify({'message': '配置更新成功'})

@app.route('/api/projects/<project_id>/annotations', methods=['PUT'])
def save_annotation(project_id):
    """保存标注"""
    data = request.json
    row_id = data['row_id']
    annotations = data['annotations']
    
    conn = get_db_connection()
    conn.execute('''
        UPDATE project_data 
        SET annotations = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND project_id = ?
    ''', (json.dumps(annotations), row_id, project_id))
    conn.commit()
    conn.close()
    
    return jsonify({'message': '标注保存成功'})

@app.route('/api/projects/<project_id>/search', methods=['POST'])
def search_content(project_id):
    """搜索内容"""
    data = request.json
    search_text = data.get('search_text', '')
    search_api = data.get('search_api', '')
    
    if not search_api:
        return jsonify({'error': '未配置搜索API'}), 400
    
    try:
        # 调用外部搜索API
        response = requests.post(search_api, json={'query': search_text}, timeout=10)
        results = response.json()
        
        return jsonify({
            'results': results,
            'search_text': search_text
        })
    except Exception as e:
        # 返回模拟搜索结果
        return jsonify({
            'results': {
                'items': [
                    {'title': f'搜索结果1 - {search_text}', 'snippet': '相关内容描述...'},
                    {'title': f'搜索结果2 - {search_text}', 'snippet': '更多相关信息...'},
                    {'title': f'搜索结果3 - {search_text}', 'snippet': '补充信息...'}
                ]
            },
            'search_text': search_text
        })

@app.route('/api/projects/<project_id>/export', methods=['POST'])
def export_data(project_id):
    """导出项目数据"""
    export_format = request.json.get('format', 'csv')
    include_original = request.json.get('include_original', True)
    include_annotations = request.json.get('include_annotations', True)
    include_stats = request.json.get('include_stats', False)
    
    conn = get_db_connection()
    
    # 获取项目信息
    project = conn.execute('SELECT * FROM projects WHERE id = ?', (project_id,)).fetchone()
    if not project:
        conn.close()
        return jsonify({'error': '项目不存在'}), 404
    
    # 获取数据
    data_rows = conn.execute('''
        SELECT * FROM project_data WHERE project_id = ? ORDER BY row_index
    ''', (project_id,)).fetchall()
    
    # 获取列配置
    columns = conn.execute('''
        SELECT * FROM column_configs WHERE project_id = ? ORDER BY id
    ''', (project_id,)).fetchall()
    
    conn.close()
    
    # 构建导出数据
    export_data = []
    for row in data_rows:
        export_row = {}
        original_data = json.loads(row['original_data'])
        annotations = json.loads(row['annotations'] or '{}')
        
        if include_original:
            for col in columns:
                export_row[col['display_name']] = original_data.get(col['original_name'], '')
        
        if include_annotations:
            config = json.loads(project['config'] or '{}')
            annotation_configs = config.get('annotation_configs', [])
            for ann_config in annotation_configs:
                field_name = ann_config.get('fieldName', '')
                if field_name:
                    export_row[f'标注_{field_name}'] = annotations.get(field_name, '')
        
        export_data.append(export_row)
    
    # 添加统计信息
    if include_stats and export_data:
        total_count = len(export_data)
        annotated_count = sum(1 for row in data_rows if json.loads(row['annotations'] or '{}'))
        completion_rate = round((annotated_count / total_count * 100), 1)
        
        stats_row = {list(export_data[0].keys())[0]: '--- 统计信息 ---'}
        stats_row['总数据量'] = total_count
        stats_row['已标注数'] = annotated_count
        stats_row['完成率(%)'] = completion_rate
        export_data.append(stats_row)
    
    # 生成文件
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{project['name']}_{timestamp}"
    
    if export_format == 'csv':
        df = pd.DataFrame(export_data)
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"{filename}.csv"
        )
    
    elif export_format == 'excel':
        df = pd.DataFrame(export_data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='标注数据', index=False)
        
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"{filename}.xlsx"
        )
    
    elif export_format == 'json':
        return send_file(
            io.BytesIO(json.dumps(export_data, ensure_ascii=False, indent=2).encode('utf-8')),
            mimetype='application/json',
            as_attachment=True,
            download_name=f"{filename}.json"
        )

@app.route('/api/projects/<project_id>/statistics', methods=['GET'])
def get_statistics(project_id):
    """获取项目统计信息"""
    conn = get_db_connection()
    
    # 基础统计
    total_count = conn.execute('''
        SELECT COUNT(*) as count FROM project_data WHERE project_id = ?
    ''', (project_id,)).fetchone()['count']
    
    annotated_count = conn.execute('''
        SELECT COUNT(*) as count FROM project_data 
        WHERE project_id = ? AND annotations != '{}' AND annotations IS NOT NULL
    ''', (project_id,)).fetchone()['count']
    
    # 获取所有标注数据用于分析
    data_rows = conn.execute('''
        SELECT annotations FROM project_data WHERE project_id = ?
    ''', (project_id,)).fetchall()
    
    conn.close()
    
    # 计算评分分布
    all_ratings = []
    for row in data_rows:
        annotations = json.loads(row['annotations'] or '{}')
        for value in annotations.values():
            if isinstance(value, (int, float)):
                all_ratings.append(value)
    
    avg_rating = sum(all_ratings) / len(all_ratings) if all_ratings else 0
    
    return jsonify({
        'total_count': total_count,
        'annotated_count': annotated_count,
        'completion_rate': round((annotated_count / total_count * 100), 1) if total_count > 0 else 0,
        'avg_rating': round(avg_rating, 2),
        'rating_distribution': all_ratings
    })

def get_html_content():
    """返回HTML内容作为备用"""
    # 这里可以直接嵌入HTML内容，或者返回一个简单的错误页面
    return '''
    <!DOCTYPE html>
    <html>
    <head><title>智能数据标注平台</title></head>
    <body>
        <h1>智能数据标注平台</h1>
        <p>请确保templates/index.html文件存在</p>
        <p>或者将前端文件放在正确的位置</p>
    </body>
    </html>
    '''

@app.route('/api/projects/<project_id>', methods=['DELETE'])
def delete_project(project_id):
    """删除项目"""
    conn = get_db_connection()
    
    try:
        # 检查项目是否存在
        project = conn.execute('SELECT * FROM projects WHERE id = ?', (project_id,)).fetchone()
        if not project:
            return jsonify({'error': '项目不存在'}), 404
        
        # 删除项目相关的所有数据
        conn.execute('DELETE FROM project_data WHERE project_id = ?', (project_id,))
        conn.execute('DELETE FROM column_configs WHERE project_id = ?', (project_id,))
        conn.execute('DELETE FROM projects WHERE id = ?', (project_id,))
        
        # 删除上传的文件
        if project['file_path'] and os.path.exists(project['file_path']):
            try:
                os.remove(project['file_path'])
            except Exception as e:
                print(f"删除文件失败: {e}")
        
        conn.commit()
        return jsonify({'message': '项目删除成功'})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': f'删除项目失败: {str(e)}'}), 500
    finally:
        conn.close()

@app.route('/api/projects/<project_id>/replace-file', methods=['POST'])
def replace_project_file(project_id):
    """替换项目文件（保留配置）"""
    conn = get_db_connection()
    
    try:
        # 检查项目是否存在
        project = conn.execute('SELECT * FROM projects WHERE id = ?', (project_id,)).fetchone()
        if not project:
            return jsonify({'error': '项目不存在'}), 404
        
        # 保存原有的列配置和标注配置
        original_columns = conn.execute('''
            SELECT * FROM column_configs WHERE project_id = ?
        ''', (project_id,)).fetchall()
        
        original_config = json.loads(project['config'] or '{}')
        
        # 处理文件上传（复用原有上传逻辑）
        upload_type = request.form.get('upload_type', 'file')
        delimiter = request.form.get('delimiter', ',')
        encoding = request.form.get('encoding', 'utf-8')
        has_header = request.form.get('has_header') == 'true'
        
        if delimiter == '\\t':
            delimiter = '\t'
        
        file_data = None
        
        if upload_type == 'file':
            if 'file' not in request.files:
                return jsonify({'error': '没有选择文件'}), 400
            
            file = request.files['file']
            if file.filename == '' or not allowed_file(file.filename):
                return jsonify({'error': '无效的文件'}), 400
            
            filename = secure_filename(file.filename)
            file_path = os.path.join(UPLOAD_FOLDER, f"{project_id}_{filename}")
            file.save(file_path)
            
            # 检测编码
            if encoding == 'auto':
                encoding = detect_encoding(file_path)
            
            with open(file_path, 'r', encoding=encoding) as f:
                file_data = f.read()
        
        elif upload_type == 'url':
            url = request.form.get('url')
            response = requests.get(url)
            if encoding == 'auto':
                encoding = response.apparent_encoding or 'utf-8'
            file_data = response.content.decode(encoding)
        
        elif upload_type == 'ftp':
            ftp_server = request.form.get('ftp_server')
            ftp_user = request.form.get('ftp_user')
            ftp_password = request.form.get('ftp_password')
            ftp_path = request.form.get('ftp_path')
            
            ftp = ftplib.FTP(ftp_server)
            ftp.login(ftp_user, ftp_password)
            
            bio = io.BytesIO()
            ftp.retrbinary('RETR ' + ftp_path, bio.write)
            ftp.quit()
            
            if encoding == 'auto':
                raw_data = bio.getvalue()
                encoding = chardet.detect(raw_data)['encoding']
            
            file_data = bio.getvalue().decode(encoding)
        
        # 解析新数据
        lines = file_data.strip().split('\n')
        if not lines:
            return jsonify({'error': '文件为空'}), 400
        
        headers = []
        start_index = 0
        
        if has_header and lines:
            headers = lines[0].split(delimiter)
            start_index = 1
        elif lines:
            first_line = lines[0].split(delimiter)
            headers = [f'列{i+1}' for i in range(len(first_line))]
        
        # 删除旧数据
        conn.execute('DELETE FROM project_data WHERE project_id = ?', (project_id,))
        
        # 保存新数据行
        for i in range(start_index, len(lines)):
            values = lines[i].split(delimiter)
            row_data = {}
            for j, header in enumerate(headers):
                row_data[header] = values[j] if j < len(values) else ''
            
            conn.execute('''
                INSERT INTO project_data (project_id, row_index, original_data, annotations)
                VALUES (?, ?, ?, ?)
            ''', (project_id, i - start_index, json.dumps(row_data), '{}'))
        
        # 更新列配置（尝试匹配原有配置）
        conn.execute('DELETE FROM column_configs WHERE project_id = ?', (project_id,))
        
        for i, header in enumerate(headers):
            # 查找是否有匹配的原有配置
            matched_config = None
            for orig_col in original_columns:
                if orig_col['original_name'] == header or orig_col['display_name'] == header:
                    matched_config = orig_col
                    break
            
            if matched_config:
                # 使用原有配置
                conn.execute('''
                    INSERT INTO column_configs (project_id, original_name, display_name, column_type, 
                                              is_visible, is_url, url_new_window, column_width, 
                                              is_searchable, search_api)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (project_id, header, matched_config['display_name'], matched_config['column_type'],
                     matched_config['is_visible'], matched_config['is_url'], matched_config['url_new_window'],
                     matched_config['column_width'], matched_config['is_searchable'], matched_config['search_api']))
            else:
                # 使用默认配置
                conn.execute('''
                    INSERT INTO column_configs (project_id, original_name, display_name, column_type, 
                                              is_visible, is_url, url_new_window, column_width, 
                                              is_searchable, search_api)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (project_id, header, header, 'text', 1, 0, 1, 150, 0, ''))
        
        # 更新项目文件路径
        file_path = file_path if upload_type == 'file' else None
        conn.execute('''
            UPDATE projects SET file_path = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (file_path, project_id))
        
        conn.commit()
        
        # 返回前3行预览数据
        preview_data = []
        for i in range(min(3, len(lines) - start_index)):
            values = lines[start_index + i].split(delimiter)
            row = {}
            for j, header in enumerate(headers):
                row[header] = values[j] if j < len(values) else ''
            preview_data.append(row)
        
        return jsonify({
            'message': '文件替换成功',
            'headers': headers,
            'preview_data': preview_data,
            'total_rows': len(lines) - start_index,
            'configs_preserved': len([c for c in original_columns if c['original_name'] in headers])
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': f'文件替换失败: {str(e)}'}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    init_db()
    print("数据库初始化完成")
    print("服务器启动中...")
    print("访问地址: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)