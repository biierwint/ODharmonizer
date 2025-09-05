# ODharmonizer: Omics Data Harmonizer
## ODharmonizer Setup Guide

ODharmonizer (Omics Data Harmonizer) is a suite of tools, containing ODmapper, ODannotator and ODconverter, used to harmonized omics data into OMOP CDM schema.

ODmapper (Omics Data Mapper) is a dockerized API service that enables mapping of omics features -- such as gene symbols, transcript identifiers, and genomic variants -- into standard `concept_id`s used in the OMOP Common Data Model (CDM), specifically targeting Omics CDM extensions. ODmapper uses the Django REST Framework and relies on external resources like SeqRepo and UTA to resolve and normalize genomic identifiers. It is preconfigured to work in an offline Docker Compose setup.

ODannotator (Omics Data Annotator) is a collection of scripts used to annotate genomic data (in VCF format) or expression (gene/transcript/protein) data (in CSV: genes x samples table) with the standard `concept_id`s. It leverages on ODmapper API service as well as a set of vocabularies preloaded into concept and concept_synonym tables of OMOP CDM. 

ODconverter (Omics Data CDM Converter) is a dockerized web apps (based on streamlit) that enables the conversion of annotated omics data to the OMOP CDM tables. It provides the insert_table.py script that enable the loading of the converted CDM into OMOP CDM tables. 


---

## Prerequisites

Ensure the following are installed on your system:

- Docker Engine: https://docs.docker.com/engine/install/
- Docker Compose: https://docs.docker.com/compose/install/
- Optional: gunzip, tar, psql client, curl for testing

Note: No GPU or external network connection is required during runtime if using the provided offline setup.

---

## 1. Clone the ODharmonizer Repository

```bash
git clone https://github.com/biierwint/ODharmonizer.git
cd ODharmonizer/
```

Expected directory structure:

```
ODharmonizer/
|-- ODmapper/               # ODmapper Project folder
|-- ODannotator/            # ODannotator Project folder
|-- ODconverter/            # ODconverter Project folder
|-- data-repositories/      # Database dump and SeqRepo data
|-- README.md               # Readme file
|-- .env.example            # Example file for setting up environment variables
```

---

## 2. Configure Environment Variables

Copy the .env.example file to .env:

```bash
cp .env.example .env
```

Edit the .env file and update the following:

- DJANGO_SECRET_KEY -- create one using:
  ```bash
  pip install django
  python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
  ```

- DJANGO_ALLOWED_HOSTS -- e.g., localhost, 127.0.0.1, or IP/domain name
- DATABASE_USERNAME / DATABASE_PASSWORD -- credentials for PostgreSQL
- SEQREPO -- the path to the unpacked seqrepo/ directory (optional if default is /usr/local/share/seqrepo)
- HOST_UID -- you can obtain this by executing 'id -u' in the shell prompt.
- HOST_GID -- you can obtain this by executing 'id -g' in the shell prompt.
- USERNAME -- you can obtain this by executing 'whoami' in the shell prompt.

---


## 3. Prepare the SeqRepo Data

We have prepared a ready to use SeqRepo dataset. You can download it from here and move it to the data-repositories/ folder:

```bash
cd data-repositories/
tar -xvf seqrepo-data-2024-12-20.tar
sudo mv seqrepo/ /usr/local/share/
sudo chmod -R +r /usr/local/share/seqrepo
```
Note: You can choose another directory, but remember to update the SEQREPO path in your .env file.

Alternatively, you can follow the instruction here to download the different version of [SeqRepo dataset](https://github.com/biocommons/biocommons.seqrepo).

---

## 4. Build docker containers
Before you build docker containers, please create three folders: tmp/, input/ and output/ inside the ODharmonizer folder.
These folders are used for ODconverter if you want to load data from the localhost.
```bash
mkdir -p tmp/ input/ output/
```

From the project ODharmonizer/ folder, build the containers by:
```bash
docker compose build
```

## 5. Start Docker Services

From the project ODharmonizer/ folder, start all services in detached mode:

```bash
docker compose up --detach
```

This launches:
- odmapper (Django API server)
- odconverter (Streamlit web application)
- odharmonizer-postgres (PostgreSQL DB)
- seqrepo-rest-service (REST interface for SeqRepo)
- uta (optional UTA splicing service)

---

## 6. Initialize the PostgreSQL Database

Restore the database schema and content:

```bash
gunzip -c data-repositories/database-setup.sql.gz | docker exec -i odharmonizer-postgres psql -U postgres
gunzip -c data-repositories/odmapper_database_dump.sql.gz | docker exec -i odharmonizer-postgres psql -U postgres -d odmapper
```

---

## 7. Apply Django Migrations

Create and apply database migrations:

```bash
docker exec -i odmapper python manage.py makemigrations
docker exec -i odmapper python manage.py migrate
```

---

## 8. Create Django Admin Superuser

To access the Django admin interface:

```bash
docker exec -it odmapper python manage.py createsuperuser
```

You will be prompted for:
- Username
- Email
- Password

Restart containers:

```bash
docker compose down
docker compose up --detach
```

---

## 9. Validate Deployment

From the same machine:

- ODmapper API: http://localhost:8000/
- Streamlit Webapps: http://localhost:8501/
- SeqRepo REST API: http://localhost:5000/

From a remote machine:

1. Edit .env and update DJANGO_ALLOWED_HOSTS
2. Restart services:
   ```bash
   docker compose down
   docker compose up --detach
   ```
3. Access from a browser: `http://<your-server-ip>:8000/`

---

## 10. Sample API Query Endpoints

```bash
curl "http://localhost:8000/api/odmapper/gene/gencode/ENSG00000139618/"
curl "http://localhost:8000/api/odmapper/synonym/ga4gh:VA.7t7vgKri49CMLMUWNF4_HW1aJltBWz87/"
```

---

## Troubleshooting and Tips

View logs: [From the ODharmonizer/ folder]
  ```bash
  docker compose logs
  ```

Access container shell:
  ```bash
  docker exec -it <container_name> sh

  Example:
  docker exec -it odmapper sh
  ```

Access PostgreSQL shell:
  ```bash
  docker exec -it odharmonizer-postgres psql -U postgres -d odmapper
  ```

---

## Optional: Enable External Volume Mounts

To persist data or adjust mount paths, edit `compose.yml` and define custom volumes or bind mounts.

---

## Final Note

You are now ready to use ODharmonizer suite to translate omics data into OMOP CDM tables. For production, consider adding HTTPS, firewall rules, and persistent volume configurations.

