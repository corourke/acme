mvn clean package
cp ./target/datagen-1.0-jar-with-dependencies.jar ./datagen-1.0.jar
if [ ! -f config.properties ] ; 
  then cp config.properties.example config.properties
  echo "Please edit config.properties as needed before running the program" 
fi
