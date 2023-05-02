Overview
========

Esta es la solución del Challenge Ingeniero de Datos Semi-Senior-2

Project Contents
================

Your Astro project contains the following files and folders:
Este proyecto contiene los siguientes archivos y carpetas:

- dags: Esta carpeta contiene los archivos python de cada flujo de Airflow, Este directorio incluye dos subdirectorios:
    - `data_process` : que contiene el flujo de trabajo relacionado a la prueba, así como sus componentes. 
    - `example_dags`: Son ejemplo basicos de flujo de trabajo que vienen por defecto en airflo.
- Dockerfile: Este archivo contiene la imagen docker de airflow 2.5.0, ademas de proporcionar la instalación de paquetes y dependecias especificas para el desarrollo de  esta prueba.
- include: Esta carpeta contiene cualquier archivo adiciona que se requiera incluir como parte del proyecto, Esta vacio por defecto.
- packages.txt: Instala paquetes en el sistema operativo necesarios para este proyecto.
- requirements.txt: Instala las dependecias de python necesarias para este proyecto.
- plugins: Añade plugins personalizado o de comunidad, para este ocasión no es necesario, por ende viene vacio.
- docker-compose.yaml: Se usa este archivo para especificar las conexiones, Variables, Base de datos, ejecutor, trabajador, etc. Con el fin de construir satisfactoriamente un contenedor localmente.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
