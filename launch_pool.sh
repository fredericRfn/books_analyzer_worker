for i in 1 $1 
    do
        java -jar worker.jar $i &
    done
echo $1 "workers have been created"
