# 🌦️ Climate Pipeline — ETL con Airflow + Docker

Pipeline de datos que extrae información climática diaria de Bogotá desde la API de Open Meteo,
la transforma con Pandas y la carga en PostgreSQL. Orquestado con Apache Airflow en Docker.

## 🏗️ Arquitectura

Open Meteo API → Airflow DAG → Python/Pandas → PostgreSQL
(Docker Compose)

## ⚙️ Stack

| Herramienta | Uso |
|---|---|
| Apache Airflow 2.8 | Orquestación del pipeline |
| Python / Pandas | Extracción y transformación |
| PostgreSQL 15 | Almacenamiento |
| Docker Compose | Infraestructura local |

## 🚀 Cómo correrlo

### Requisitos
- Docker Desktop instalado
- Git

### Pasos


## 1. Clonar el repositorio
```bash
git clone https://github.com/tu-usuario/clima-pipeline.git
cd clima-pipeline
```
## 2. Inicializar Airflow
```bash
docker-compose run --rm airflow-init
```
```bash
## 3. Levantar el entorno
docker-compose up -d
```
## 4. Abrir la UI de Airflow
```bash
# http://localhost:8080 — usuario: admin, contraseña: admin
```
## 5. Activar y triggerear el DAG "clima_bogota_etl"


## 📊 Datos que captura

| Campo | Descripción |
|---|---|
| `fecha` | Fecha del registro |
| `temp_max` | Temperatura máxima del día (°C) |
| `temp_min` | Temperatura mínima del día (°C) |
| `temp_promedio` | Promedio calculado en transformación |
| `precipitacion_mm` | Precipitación acumulada (mm) |
| `ingested_at` | Timestamp de carga al DW |

## 🔍 Verificar datos en Postgres

```bash
docker-compose exec postgres psql -U airflow -d clima_db \
  -c "SELECT * FROM clima_bogota ORDER BY fecha DESC LIMIT 10;"
```

## 📁 Estructura del proyecto

clima-pipeline/
├── docker-compose.yml    # Infraestructura: Airflow + Postgres
├── dags/
│   └── clima_dag.py      # DAG principal con tasks ETL
├── requirements.txt      # Dependencias Python
└── README.md

## 💡 Aprendizajes clave

- Configuración de Airflow con Docker en modo producción
- Diseño de DAGs con manejo de errores y retries
- Serialización de datos entre tasks via XCom
- Carga idempotente con `ON CONFLICT` en Postgres