from flask import Flask, render_template, request, jsonify, redirect, url_for
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
import pandas as pd
import os
from .config import Config
from dotenv import load_dotenv
from flask_apscheduler import APScheduler
from flask_mail import Mail, Message
from datetime import datetime, timedelta

load_dotenv()

app = Flask(__name__)
app.config.from_object(Config)

# Email configuration
app.config.update(
    MAIL_SERVER='smtp.gmail.com',
    MAIL_PORT=587,
    MAIL_USE_TLS=True,
    MAIL_USE_SSL=False,
    MAIL_USERNAME=os.getenv('MAIL_USERNAME'),  # Your email
    MAIL_PASSWORD=os.getenv('MAIL_PASSWORD'),   # Your email password or app password
    MAIL_DEFAULT_SENDER=os.getenv('MAIL_USERNAME')
)

mail = Mail(app)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

# Define the email alert function
def send_alert_email(project_name):
    with app.app_context():
        msg = Message(
            'Alert: High Volume of Tweets Awaiting Review',
            sender=os.getenv('MAIL_USERNAME'),
            recipients=['natasha.jacobs@signify.ai', 'jonathan.hirshler@signify.ai','georgia.relf@signify.ai']  # Replace with your email or a list of emails
        )
        msg.body = f'The project "{project_name}" has exceeded 200 tweets awaiting first stage review in the last 30mins.'
        mail.send(msg)
        
# Define the scheduled job to check tweet counts
@scheduler.task('interval', id='check_tweet_counts', minutes=30)
def check_tweet_counts():
    time_threshold = datetime.now() - timedelta(minutes=30)
    query = text("""
        SELECT p.name, COUNT(tr.id) AS awaiting_1st_review
        FROM projects p
        JOIN streams s ON s.projectref = p.ref
        JOIN rule_record rr ON rr.stream = s.ref
        JOIN rule_tweets rt ON rt.rule_id = rr.id
        JOIN tweet_record tr ON tr.id = rt.tweet_id
        WHERE p.clientref = 'paris_2024_olympics'
        AND tr.reviewed_h1 = FALSE
        AND tr.threat > 0
        AND tr.dtime >= :time_threshold
        GROUP BY p.name
    """)
    result = db_session.execute(query, {"time_threshold": time_threshold}).fetchall()
    for row in result:
        project_name, awaiting_1st_review = row
        if awaiting_1st_review >= 200:
            send_alert_email(project_name)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/contact')
def contact():
    return render_template('contact.html')

@app.route('/client/<client_ref>')
def client_projects(client_ref):
    try:
        client_projects_status = get_client_projects_status(client_ref)
        client_name = client_ref.replace('_', ' ').title()
        return render_template('client_projects.html', client_projects_status=client_projects_status, client_name=client_name)
    except Exception as e:
        print(f"Error in client_projects: {e}")
        return render_template('error.html', error=str(e))

@app.route('/filter', methods=['POST'])
def filter_projects():
    client_ref = request.form.get('client_ref')
    return redirect(url_for('client_projects', client_ref=client_ref))

def get_projects_for_client(client_ref, search_query=None):
    try:
        params = {"client_ref": client_ref}
        query = "SELECT ref, name FROM projects WHERE clientref = :client_ref"
        if search_query:
            query += " AND name ILIKE :search_query"
            params["search_query"] = f"%{search_query}%"
        query += " ORDER BY name"
        print(f"Query: {query}, Params: {params}")  # Debug print
        return pd.read_sql(text(query), engine, params=params).to_dict(orient='records')
    except Exception as e:
        print(f"Error in get_projects_for_client: {e}")
        return []

def get_client_projects_status(client_ref):
    try:
        query = text("""
            SELECT
                p.name,
                COUNT(DISTINCT tr.id) AS total_tweets,
                COUNT(DISTINCT CASE
                                 WHEN tr.reviewed_h1 = false
                                 AND tr.threat > 0
                                 THEN tr.id
                                 ELSE NULL
                              END) AS awaiting_1st_review,
                COUNT(DISTINCT CASE
                                 WHEN tr.reviewed_h2 = false
                                 AND tr.abusive_h1 = true
                                 AND tr.threat > 0
                                 THEN tr.id
                                 ELSE NULL
                              END) AS awaiting_2nd_review
            FROM projects p
            LEFT JOIN streams s ON s.projectref = p.ref
            LEFT JOIN rule_record rr ON rr.stream = s.ref
            LEFT JOIN rule_tweets rt ON rt.rule_id = rr.id
            LEFT JOIN tweet_record tr ON tr.id = rt.tweet_id
            WHERE p.clientref = :client_ref
            GROUP BY p.name
        """)
        result = db_session.execute(query, {"client_ref": client_ref}).fetchall()
        return [{"name": row[0], "awaiting_1st_review": row[2], "awaiting_2nd_review": row[3]} for row in result]
    except Exception as e:
        print(f"Error in get_client_projects_status: {e}")
        return []

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
        try:
            count = pd.read_sql(query, engine).iloc[0]['count']
            stage_counts[stage] = int(count)
        except Exception as e:
            print(f"Error in get_project_status for stage {stage}: {e}")
            stage_counts[stage] = 0
    
    return stage_counts

@app.route('/project/<project_ref>')
def project_status(project_ref):
    try:
        status = get_project_status(project_ref)
        return render_template('project_status.html', project_ref=project_ref, status=status)
    except Exception as e:
        print(f"Error in project_status: {e}")
        return render_template('error.html', error=str(e))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
