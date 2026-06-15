import pendulum
from airflow import DAG
from airflow.decorators import task
import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook


data_path = "/mnt/shared/andy-output/test.dat"
patients_path = "/mnt/shared/andy-output/patients.dat"


with DAG(
    dag_id="mini-dag",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["audio", "processing"],
    catchup=False,
    access_control={"Admin": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
) as dag:
    
    @task()
    def extract_data(**kwargs):
        """Extrahiert Rohdaten."""
        print("Daten werden extrahiert...")
        return {"user_count": 100, "status": "success"}

    @task()
    def process_data(extracted_data):
        """Verarbeitet die extrahierten Daten."""
        print(f"Verarbeite Daten mit User Count: {extracted_data['user_count']}")
        processed_count = extracted_data["user_count"] * 2
        return processed_count

    @task()
    def load_data(*val):
        processed_count = val[0]
        """Speichert die verarbeiteten Daten."""
        print(f"Lade finale Datenmenge: {processed_count}")

        with open(data_path, "a") as file:
            date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
            file.write(f"{date}\tLade finale Datenmenge:\t{processed_count}\n")

    @task()
    def write_patient_data(**context):
        hook = MySqlHook(mysql_conn_id='ddmariadb')
        records = hook.get_records(
            "SELECT id, patient_name, date_of_birth, visit_time, severity, primary_diagnosis, secondary_diagnoses, recommended_tests, recommended_treatment, follow_up FROM triageai"
        )
        with open(patients_path, "w") as file:
            for row in records:
                date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                file.write(f"{date}\t{row}\n")

    # Definiere die Task-Abhängigkeiten
    raw_data = extract_data()
    clean_data = process_data(raw_data)
    load_data(clean_data, write_patient_data())
    
    
