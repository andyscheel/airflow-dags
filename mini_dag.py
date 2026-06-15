import pendulum
from airflow import DAG
from airflow.decorators import task
import datetime

data_path = "/mnt/shared/andy-output/test.dat"


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
    def load_data(processed_count):
        """Speichert die verarbeiteten Daten."""
        print(f"Lade finale Datenmenge: {processed_count}")

        with open(data_path, "a") as file:
            date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
            file.write(f"{date}\tLade finale Datenmenge:\t{processed_count}\n")

    # Definiere die Task-Abhängigkeiten
    raw_data = extract_data()
    clean_data = process_data(raw_data)
    load_data(clean_data)
