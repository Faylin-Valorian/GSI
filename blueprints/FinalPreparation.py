import os
from flask import Blueprint, request, jsonify, Response, stream_with_context
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties

final_prep_bp = Blueprint('final_prep', __name__)

# SQL Queries from your uploads
QUERIES = [
    {
        "name": "Generic Legal Other Insert",
        "sql": """
            insert into genericdataimport (fn, col01varchar, stech_image_path, legal_type, col20other, deleteFlag, instrumentid) 
            select replace(fn, 'HEADER', 'Legal'), col01varchar, stech_image_path, 'Other', 'NO LEGAL', 'FALSE', instrumentid 
            from genericdataimport where fn like '%HEADER%' and deleteFlag = 'FALSE' and instrumentid not in (select instrumentid from genericdataimport where fn like '%legal%' and deleteFlag = 'FALSE')
        """
    },
    {
        "name": "Keli Page Count",
        "sql": """
            IF EXISTS (SELECT * FROM sysobjects WHERE name = 'KeliPageCount') DROP TABLE KeliPageCount
            SELECT COUNT(pages.id) AS pagesCount, pages.instrumentid AS instrumentid INTO KeliPageCount FROM GenericDataImport pages, GenericDataImport a
            WHERE pages.fn LIKE '%image%' AND a.fn LIKE '%header%' AND pages.instrumentid = a.instrumentid GROUP BY pages.instrumentid
        """
    },
    {
        "name": "Keli Pages Internal",
        "sql": """
            IF EXISTS (SELECT * FROM sysobjects WHERE name = 'KeliPagesInternal') DROP TABLE KeliPagesInternal
            SELECT *, book + '\\' + page_number + '.TIF' as path INTO KeliPagesInternal FROM fromkellpropages WHERE replace(book, 'MS', '00') BETWEEN '{0}' AND '{1}'
        """
    },
    {
        "name": "Keli Beg End Page Numbers",
        "sql": """
            IF EXISTS (SELECT * FROM sysobjects WHERE name = 'KeliBegEndPageNumbers') DROP TABLE KeliBegEndPageNumbers
            SELECT a.instrumentid AS instrumentid, MIN(a.page_number) AS beginning_page, MAX(a.page_number) AS ending_page INTO KeliBegEndPageNumbers FROM GenericDataImport a, GenericDataImport b
            WHERE a.fn LIKE '%image%' AND b.fn LIKE '%header%' AND a.instrumentid = b.instrumentid GROUP BY a.instrumentid
        """
    },
    {
        "name": "Party Suffix Count",
        "sql": """
            IF EXISTS (SELECT * FROM sysobjects WHERE name = 'partySuffixCount') DROP TABLE partySuffixCount
            select
                a.instrumentid,
                count(a.col02varchar) as nameCount,
                a.col02varchar as nameSuffix
            into partySuffixCount from genericdataimport a, genericdataimport b
                where a.fn like '%name%' and b.fn like '%header%' and a.deleteFlag = 'FALSE' and b.deleteFlag = 'FALSE' and a.instrumentid = b.instrumentid and a.col02varchar = 'Grantor'
                or a.fn like '%name%' and b.fn like '%header%' and a.deleteFlag = 'FALSE' and b.deleteFlag = 'FALSE' and a.instrumentid = b.instrumentid and a.col02varchar = 'Grantee'
            group by a.instrumentid, a.col02varchar
        """
    },
    {
        "name": "Keli Grantor Grantee Suffix",
        "sql": """
            IF EXISTS (SELECT * FROM sysobjects WHERE name = 'KeliGrantorGranteeSuffix') DROP TABLE KeliGrantorGranteeSuffix
            SELECT
            a.instrumentid , CASE
                    WHEN (SELECT nameCount FROM partySuffixCount b WHERE nameSuffix = 'Grantor' AND a.instrumentid = b.instrumentid ) > 1
                            THEN (SELECT id FROM fromkellproparty_suffixes WHERE name = 'et al') ELSE ''
            END AS grantor_suffix_internal_id, CASE
                    WHEN (SELECT nameCount FROM partySuffixCount c WHERE nameSuffix = 'Grantee' AND a.instrumentid = c.instrumentid ) > 1
                            THEN (SELECT id FROM fromkellproparty_suffixes WHERE name = 'et al') ELSE ''
            END AS grantee_suffix_internal_id INTO KeliGrantorGranteeSuffix FROM GenericDataImport a WHERE fn LIKE '%header%'
        """
    }
]

@final_prep_bp.route('/api/tools/final-preparation/execute', methods=['POST'])
@login_required
def execute_final_prep():
    if current_user.role != 'admin': return jsonify({'message': 'Unauthorized'}), 403
    
    data = request.json
    county_id = data.get('county_id')
    book_start = data.get('book_start', '000000')
    book_end = data.get('book_end', '999999')

    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'message': 'County not found'}), 404

    # Replace placeholders in SQL
    processed_queries = []
    for q in QUERIES:
        sql = q['sql']
        # KeliPagesInternal uses {0} and {1}
        if '{0}' in sql:
            sql = sql.replace('{0}', book_start).replace('{1}', book_end)
        
        # Replace table names with county specific names if needed (assuming these are shared tables or specific to county context?)
        # Based on your previous tools, you often prefix tables. 
        # However, these scripts use "GenericDataImport" (main table) and create new tables (e.g. KeliPageCount).
        # Assuming standard behavior is running this ON the county's database connection.
        
        # Specific replacements for 'fromkellpropages' and 'fromkellproparty_suffixes' if they are county specific
        sql = sql.replace('fromkellpropages', f"{c.county_name}_keli_pages")
        sql = sql.replace('fromkellproparty_suffixes', f"{c.county_name}_keli_party_suffixes")
        
        processed_queries.append({'name': q['name'], 'sql': sql})

    def generate():
        import json
        yield json.dumps({'type': 'log', 'message': f'Starting Final Preparation for {c.county_name}...'}) + '\n'
        
        try:
            with db.session.begin():
                for step in processed_queries:
                    yield json.dumps({'type': 'log', 'message': f"Running: {step['name']}..."}) + '\n'
                    db.session.execute(text(step['sql']))
                    yield json.dumps({'type': 'log', 'message': f"Completed: {step['name']}"}) + '\n'
            
            db.session.commit()
            yield json.dumps({'type': 'complete', 'message': 'Final Preparation Completed Successfully.'}) + '\n'
        except Exception as e:
            db.session.rollback()
            yield json.dumps({'type': 'error', 'message': str(e)}) + '\n'

    return Response(stream_with_context(generate()), mimetype='application/json')