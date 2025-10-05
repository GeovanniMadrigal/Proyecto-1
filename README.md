# 🚀 ETL Pipeline

Un proyecto completo de ingeniería de datos que implementa un pipeline ETL usando Docker, Airflow y PostgreSQL.

# Arquitectura del Sistema

![Arquitectura](docs/architecture.png)

## Tecnologías Utilizadas

- **Apache Airflow** - Orquestación de pipelines
- **PostgreSQL** - Almacenamiento de datos
- **Docker & Docker Compose** - Contenerización
- **Python** - Procesamiento de datos
- **Pandas** - Transformación de datos

## Quick Start

### Prerrequisitos
- Docker
- Docker Compose

### Ejecución
```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/data-engineer-project.git
cd data-engineer-project

# Iniciar los servicios
docker-compose up --build

# Acceder a las interfaces:
# Airflow: http://localhost:8080 (admin/admin)
# PostgreSQL: localhost:5432