# HKEX_Snapshotter
Capture short selling data daily and store to mysql db

1. Set the credential in config/db.json
2. In ubuntu environment , use crontab to set the scheduler
```
crontab -e
```
3. Place the following command into crontab
```
50 4 * * 1,2,3,4,5 python3.8 /home/ubuntu/HKEX_Snapshotter/app.py PROD AM
50 8 * * 1,2,3,4,5 python3.8 /home/ubuntu/HKEX_Snapshotter/app.py PROD PM
```
