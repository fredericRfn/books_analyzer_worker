if $# -eq 1 then
    for (( i=1; i <= $1; ++i ))
    do
        java -jar worker.jar i &
    done
    echo $1 "workers have been created"
else then
    echo "Argument missing: ex: launch_pool.sh 5"
fi
