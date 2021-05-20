#modified version https://stackoverflow.com/a/36337403/4779586, Márcio Souza Júnior
git stash
git pull
git stash pop
git add *
git commit -a -m "DailyCommit"
git push