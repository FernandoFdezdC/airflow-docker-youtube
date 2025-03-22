# AIRFLOW-DOCKER-YOUTUBE

Implements ETL using Airflow for retrieving YouTube data daily and storing it in a database.

## INSTALLATION

1. **Obtain an API key**:  
   Visit the following link to obtain an API key:  
   [https://console.cloud.google.com/apis/dashboard?inv=1&invt=AbnRmg&project=youtube-data-extraction-448115](https://console.cloud.google.com/apis/dashboard?inv=1&invt=AbnRmg&project=youtube-data-extraction-448115).  
   - Create a project and link it to `YouTube API v3`.  
   - Generate an `API key` and store it in `dags/API_keys.py` with the name `DEVELOPER_KEY`.  
   - Note that `YouTube API v3` has a limited quota that could be exhausted (see:  
     - [Quota Calculator](https://developers.google.com/youtube/v3/determine_quota_cost)  
     - [Quota and Compliance Audits](https://developers.google.com/youtube/v3/guides/quota_and_compliance_audits)).

2. **Create necessary directories**:  
   Create empty directories `logs`, `config`, and `plugins` in the root folder if they are not already created.

3. **Mount images**:  
   To mount all images, execute the following commands (in the root project folder):  
   ```bash  
   docker-compose up airflow-init
   docker-compose up

4. **Use Apache Airflow**:
   To use `Apache Airflow`, visit http://localhost:8080, and register with default user and password `airflow` and `airflow`.

## COMMENTS

1. It is safer that wherever we want to assert that a directory exists with Apache Airflow we do so with `exist_ok=True`:

```bash
   # Assert that load directories exist
   if not os.path.exists(path/to/load/directory):
      os.makedirs(path/to/load/directory, exist_ok=True)
```

That is the case because when multiple tasks or threads try to create the same directory simultaneously, a race condition can occur, potentially leading to an error if the directory is created by one task while another is still trying.