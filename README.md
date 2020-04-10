# OpenMediation Data Collector

OpenMediation Data Collector is used for pulling data from AdNetworks API  
This service should run singleton.  

## Usage

### Packaging

You can package using [mvn](https://maven.apache.org/).

```
mvn clean package -Dmaven.test.skip=true
```

After packaging is complete, you can find "on-adc.jar" in the directory "target".  
"om-adc.jar" is a executable jar, see [springboot](https://spring.io/projects/spring-boot/)

### Configuration

"application-prod.yml"  
replace auth.domain to [om-ds-server](https://github.com/AdTiming/OM-DS-Server) host:port  

```yaml
auth:
  dir: auth
  domain: {om-ds-server}
```

"om-adc.conf"

```shell script
## springboot cfg ###
MODE=service
APP_NAME=om-adc
#JAVA_HOME=/usr/local/java/jdk
JAVA_OPTS="-Dapp=$APP_NAME\
 -Duser.timezone=UTC+8\
 -Xmx3g\
 -Xms1g\
 -server"

RUN_ARGS="--spring.profiles.active=prod"
PID_FOLDER=log
LOG_FOLDER=log
LOG_FILENAME=stdout.log
```

### Run

put "om-adc.conf" and "om-adc.jar" in the same directory.

```shell script
mkdir -p log
./om-adc.jar start
```

### Logs

```shell script
tail -f log/stdout.log
```

### Stop

```shell script
./om-adc.jar stop
```

### Restart

```shell script
./om-adc.jar restart
```


