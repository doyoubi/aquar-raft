pids=""
i=0
for port in $(seq 9000 9002); do
    ( python run.py "n$i" "localhost" $port ) &
    i=$((i+1))
    pids="$pids $!"
done
trap "kill -15 $pids" INT

wait
echo $pids
