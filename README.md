# aquar-raft

### How To Play

    $ pip install -r req.txt
    $ ./run.sh
    $ for p in {9000..9002}; do redis-cli -p $p aquar raft info; done
    $ redis-cli -p <leader-port> set key value
    $ for p in {9000..9002}; do redis-cli -p $p dump; done
