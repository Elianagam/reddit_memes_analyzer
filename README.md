# Distribuidos1_tp2

## Script docker-compose

El script para generar el docker compose pudiendo indicar la cantidad de workers a utilizar para los filtros y para los reducer como tambien la cantidad de datos a enviar en cada chucks se puede correr con el siguiente comando:

```
python3 docker_compose.py <num_reducers> <worker_filter_comments> <worker_filter_posts> <chunksize>
```

Ejemplo
```
python3 docker_compose.py 3 2 2 500
```

## Run docker-compose

Se creo un make file para poder buildear las diferentes imagenes que luego se utilizaran para levantar los distintos servicios del docker compose.

**Build image**
Se buildea por separado la imagen del cliente que es quien va a copiar los archivos de data a su container ya que toma mayor cantidad de tiempo
```
make docker-client-image
```
Luego se buildean el resto de las imagenes
```
make docker-system-image
```

**Run**
Para correr el docker compose y poder visualizar los logs se corren en secuencia los siguientes comandos:
```
make docker-compose-up
```
y para el cliente con diferentes env files
```
docker run --env-file client/.env  --network=reddit_memes_analyzer_rabbitmq "client:latest"
docker run --env-file client/c2.env  --network=reddit_memes_analyzer_rabbitmq "client:latest"
docker run --env-file client/c3.env  --network=reddit_memes_analyzer_rabbitmq "client:latest"
```

**Stop**
```
make docker-compose-down
make docker-client-down
```
