from flask import Flask, render_template, request, jsonify, url_for
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
import pandas as pd
import os
from .config import Config
from dotenv import load_dotenv
from flask_apscheduler import APScheduler

load_dotenv()

app = Flask(__name__)
app.config.from_object(Config)

engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

@app.route('/')
def index():
    client_ref = "paris_2024_olympics"
    search_query = request.args.get('q', '')
    projects = get_projects_for_client(client_ref, search_query)
    total = get_total_projects_for_client(client_ref)
    return render_template('index.html', projects=projects, total=total)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/contact')
def contact():
    return render_template('contact.html')

@app.route('/suggest')
def suggest():
    client_ref = request.args.get('client_ref', '')
    search_query = request.args.get('q', '')
    projects = get_projects_for_client(client_ref, search_query)
    return jsonify(projects)

@app.route('/client/<client_ref>')
def client_projects(client_ref):
    client_projects_status = get_client_projects_status(client_ref)
    client_name = client_ref.replace('_', ' ').title()
    return render_template('client_projects.html', client_projects_status=client_projects_status, client_name=client_name)

def get_projects_for_client(client_ref, search_query=None):
    params = {"client_ref": client_ref}
    query = "SELECT ref, name FROM projects WHERE clientref = :client_ref"
    if search_query:
        query += " AND name ILIKE :search_query"
        params["search_query"] = f"%{search_query}%"
    query += " ORDER BY name"
    return pd.read_sql(text(query), engine, params=params).to_dict(orient='records')

def get_total_projects_for_client(client_ref):
    query = text("SELECT COUNT(*) FROM projects WHERE clientref = :client_ref")
    return db_session.execute(query, {"client_ref": client_ref}).scalar()

def get_project_status(project_ref, start_date=None, end_date=None):
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND tr.dtime BETWEEN '{start_date}' AND '{end_date}'"

    stages = {
        'awaiting_1st_review': "reviewed_h1 = FALSE AND threat > 0",
        'awaiting_2nd_review': "reviewed_h2 = FALSE AND abusive_h1 = TRUE AND threat > 0"
    }
    
    stage_counts = {}
    for stage, condition in stages.items():
        query = f"""
        SELECT COUNT(DISTINCT tr.id) as count
        FROM tweet_record tr
        JOIN rule_tweets rt ON tr.id = rt.tweet_id
        JOIN rule_record rr ON rt.rule_id = rr.id
        JOIN streams s ON rr.stream = s.ref
        JOIN projects p ON s.projectref = p.ref
        WHERE p.ref = '{project_ref}' AND ({condition}) {date_filter};
        """
        count = pd.read_sql(query, engine).iloc[0]['count']
        stage_counts[stage] = int(count)
    
    return stage_counts

@app.route('/project/<project_ref>')
def project_status(project_ref):
    status = get_project_status(project_ref)
    return render_template('project_status.html', project_ref=project_ref, status=status)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
