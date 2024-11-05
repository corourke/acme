mvn clean package
if [ ! -f config.properties ] ; 
  then cp config.properties.example config.properties
  chmod 600 config.properties
  echo "Please edit config.properties as needed before running the program" 
fi
