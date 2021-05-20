#modified version https://stackoverflow.com/a/36337403/4779586, Márcio Souza Júnior
@ECHO OFF
SET /p comment=DailyCommit
git pull
git add ../*
git commit -a -m "%comment%"
git push