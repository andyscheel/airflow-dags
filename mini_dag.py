import pendulum
from airflow.decorators import dag, task

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mini-dag", "airflow-3"],
)
def minimal_dag():
    
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

    # Definiere die Task-Abhängigkeiten
    raw_data = extract_data()
    clean_data = process_data(raw_data)
    load_data(clean_data)

# Instanziiere den DAG
mini_dag = minimal_dag()