sudo mkdir -p /datasets/blueteam/
sudo cp -R ./csv /datasets/blueteam/
#TODO make sure hive permissions work
sudo chown -R hive:hive /datasets/blueteam/
spark-shell -i scripts/dbsetup.scala
