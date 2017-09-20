# DEPLOY AMBARI, HDB TO AWS

## Deployment Scripts

To configure hosts and deploy ambari to a cluster run the scripts in order

000 -> Configure hostnames
001 -> Authentication setup
002 -> Config
003 -> Disk setup
004 -> Upload Ambari and HDB, install Ambari
005 -> Setup Ambari via Web interface (Note: Export HDB_ARTIFACTS if hdb and hdb ambari plugin are not in ~/workspace/artifacts/HDB)
006 -> HAWQ specific post install steps


