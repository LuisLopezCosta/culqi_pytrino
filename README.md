# Conexión a Data Lake (Culqi) desde python

## Paso 1: Genera el archivo .env

Genera un archivo llamado ".env" en la base de tu proyecto. Este archivo contendrá datos sensibles por lo tanto **NO** debe ser compartido ni subido a Github o Gitlab.

*.env*
```
HOST=<coloca el host del datalake>
PORT=<coloca el puerto del datalake>
USER=<coloca tu usuario>
PASS=<coloca tu contraseña>
```

## Paso 2: Genera tu entorno virtual para tu proyecto (Opcional)

Te recomendamos que generemos un entorno virtual por proyecto para que mantengas orden en el uso de tus librerías.

*Comandos para generar un entorno virtual*
```
pip install virtualenv
virtualenv venv
.\venv\Scripts\activate.bat
```

## Paso 3: Instala las librerías base

Ahora se instalarán todas las librerías base para que puedas realizar consultas desde python

*Comandos para instalar librerías base*
```
pip install -r requirements.txt
```

## Fuentes adicionales (para curiosos)
* https://trino.io/docs/current/develop/client-protocol.html
* https://github.com/trinodb/trino-python-client

