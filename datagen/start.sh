if [ `uname -s` = "Linux" ]; then
  sudo timedatectl set-timezone America/Los_Angeles
fi
java -jar datagen-1.0.jar
